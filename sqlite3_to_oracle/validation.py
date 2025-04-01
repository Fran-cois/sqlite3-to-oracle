"""
Module de validation pour comparer les schémas et données entre bases SQLite et Oracle.

Ce module fournit des fonctionnalités pour valider qu'une base de données SQLite
a été correctement importée dans Oracle, en comparant les schémas et les données.
"""
import os
import sys
import io
import time
import glob
import datetime
from typing import Dict, List, Optional, Tuple, Any

from . import logger
from .schema_validator import run_validation
from .oracle_utils import (
    export_validation_report, 
    get_oracle_username_from_filepath,
    check_oracle_connection
)
from .rich_logging import (
    print_title,
    print_success_message,
    print_error_message,
    print_exception
)

def validate_credentials(admin_config: Dict[str, str], new_username: str, new_password: str) -> Tuple[bool, str]:
    """
    Valide les identifiants administrateur et simule la création d'utilisateur.
    
    Args:
        admin_config: Configuration Oracle administrateur
        new_username: Nom du nouvel utilisateur à créer
        new_password: Mot de passe du nouvel utilisateur à créer
        
    Returns:
        Tuple contenant (résultat de validation, message)
    """
    admin_success, admin_message = check_oracle_connection(admin_config)
    if not admin_success:
        return False, f"Échec de la connexion administrateur Oracle: {admin_message}"
    
    import oracledb
    try:
        admin_conn = oracledb.connect(
            user=admin_config["user"],
            password=admin_config["password"],
            dsn=admin_config["dsn"]
        )
        cursor = admin_conn.cursor()
        
        cursor.execute("SELECT PRIVILEGE FROM SESSION_PRIVS WHERE PRIVILEGE = 'CREATE USER'")
        admin_has_create_user = bool(cursor.fetchone())
        
        if not admin_has_create_user:
            return False, f"L'utilisateur administrateur {admin_config['user']} n'a pas le privilège CREATE USER nécessaire pour créer un nouvel utilisateur"
        
        try:
            cursor.execute(f"SELECT 1 FROM DBA_USERS WHERE USERNAME = UPPER('{new_username}')")
            user_exists = bool(cursor.fetchone())
            
            if user_exists:
                test_config = {
                    "user": new_username,
                    "password": new_password,
                    "dsn": admin_config["dsn"]
                }
                user_success, user_message = check_oracle_connection(test_config)
                
                if not user_success:
                    return False, f"L'utilisateur {new_username} existe mais le mot de passe fourni est incorrect: {user_message}"
                
                return True, f"Utilisateur {new_username} validé avec succès (utilisateur existant)"
            else:
                cursor.execute("SELECT PRIVILEGE FROM SESSION_PRIVS WHERE PRIVILEGE = 'GRANT ANY PRIVILEGE'")
                can_grant_privileges = bool(cursor.fetchone())
                
                cursor.execute("SELECT PRIVILEGE FROM SESSION_PRIVS WHERE PRIVILEGE IN ('CREATE SESSION', 'GRANT ANY PRIVILEGE')")
                can_grant_session = bool(cursor.fetchone())
                
                if not can_grant_privileges:
                    cursor.execute("SELECT PRIVILEGE FROM SESSION_PRIVS WHERE PRIVILEGE = 'GRANT ANY ROLE'")
                    can_grant_roles = bool(cursor.fetchone())
                    
                    if not can_grant_roles:
                        return True, f"L'utilisateur administrateur {admin_config['user']} a des droits limités mais devrait pouvoir créer un utilisateur"
                
                return True, f"L'utilisateur {new_username} peut être créé par l'administrateur {admin_config['user']}"
                
        except oracledb.DatabaseError as e:
            error, = e.args
            if "ORA-00942" in str(error):
                try:
                    cursor.execute(f"SELECT 1 FROM ALL_USERS WHERE USERNAME = UPPER('{new_username}')")
                    user_exists = bool(cursor.fetchone())
                    
                    if user_exists:
                        test_config = {
                            "user": new_username,
                            "password": new_password,
                            "dsn": admin_config["dsn"]
                        }
                        user_success, user_message = check_oracle_connection(test_config)
                        
                        if not user_success:
                            return False, f"L'utilisateur {new_username} existe mais le mot de passe fourni est incorrect: {user_message}"
                        
                        return True, f"Utilisateur {new_username} validé avec succès (utilisateur existant)"
                    else:
                        return True, f"L'utilisateur administrateur {admin_config['user']} peut se connecter, tentative de création d'utilisateur"
                except:
                    return True, f"Vérification des privilèges limitée, mais connexion admin OK"
            return False, f"Erreur lors de la vérification de l'utilisateur: {error.message}"
            
    except Exception as e:
        return False, f"Erreur lors de la validation des identifiants: {str(e)}"
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'admin_conn' in locals():
            admin_conn.close()

def validate_single_schema(
    sqlite_db_path: str,
    user_config: Dict[str, str],
    verbose: bool = False
) -> Tuple[bool, Optional[str], Dict[str, Any]]:
    """
    Valide le schéma d'une seule base de données SQLite par rapport à son équivalent Oracle.
    
    Args:
        sqlite_db_path: Chemin vers le fichier SQLite source
        user_config: Configuration de connexion Oracle (user, password, dsn)
        verbose: Activer le mode verbose
        
    Returns:
        Tuple (succès, chemin du rapport, statistiques) où le chemin peut être None si l'export a échoué
    """
    # Capturer la sortie de validation pour l'exporter en Markdown
    original_stdout = sys.stdout
    validation_output = io.StringIO()
    sys.stdout = validation_output
    
    try:
        # Exécuter la validation
        validation_results = run_validation(
            sqlite_db_path,
            user_config,
            verbose=verbose
        )
        
        # Restaurer stdout
        sys.stdout = original_stdout
        
        # Récupérer le contenu du rapport
        report_content = validation_output.getvalue()
        
        # Extraire les statistiques de complétion
        stats = extract_completion_stats(report_content)
        
        # Exporter le rapport au format Markdown
        report_file = export_validation_report(report_content, sqlite_db_path)
        
        if report_file:
            logger.info(f"Rapport de validation exporté dans {report_file}")
            return True, report_file, stats
        else:
            # Si l'exportation a échoué mais que nous avons des résultats, les afficher
            print(report_content)
            return True, None, stats
    except Exception as e:
        sys.stdout = original_stdout
        print_error_message(f"Erreur lors de la validation: {str(e)}")
        if verbose:
            print_exception(e)
        return False, None, {}

def extract_completion_stats(report_content: str) -> Dict[str, Any]:
    """
    Extrait les statistiques de complétion à partir du contenu du rapport.
    
    Args:
        report_content: Contenu du rapport de validation
        
    Returns:
        Dictionnaire contenant les statistiques de complétion
    """
    import re
    
    stats = {
        "tables_sqlite": 0,
        "tables_oracle": 0,
        "table_completion": 0.0,
        "rows_sqlite": 0,
        "rows_oracle": 0,
        "data_completion": 0.0,
        "overall_completion": 0.0,
        "conversion_success": 0.0  # Nouvelle statistique pour le taux de conversion réussie
    }
    
    # Rechercher le nombre de tables
    tables_pattern = re.compile(r"Tables SQLite:\s+(\d+)\s+Tables Oracle:\s+(\d+)")
    tables_match = tables_pattern.search(report_content)
    if tables_match:
        stats["tables_sqlite"] = int(tables_match.group(1))
        stats["tables_oracle"] = int(tables_match.group(2))
        if stats["tables_sqlite"] > 0:
            stats["table_completion"] = stats["tables_oracle"] / stats["tables_sqlite"] * 100
    
    rows_sqlite = 0
    rows_oracle = 0
    
    total_pattern = re.compile(r"Total.*?lignes.*?SQLite\D+(\d+).*?Oracle\D+(\d+)", re.DOTALL | re.IGNORECASE)
    total_match = total_pattern.search(report_content)
    if total_match:
        rows_sqlite = int(total_match.group(1))
        rows_oracle = int(total_match.group(2))
    
    if rows_sqlite == 0:
        sqlite_total = re.search(r"Lignes au total dans SQLite:\s+(\d+)", report_content)
        oracle_total = re.search(r"Lignes au total dans Oracle:\s+(\d+)", report_content)
        
        if sqlite_total and oracle_total:
            rows_sqlite = int(sqlite_total.group(1))
            rows_oracle = int(oracle_total.group(1))
    
    if rows_sqlite == 0:
        sqlite_numbers = re.findall(r"(?:lignes|rows).*?SQLite.*?(\d+)", report_content, re.IGNORECASE)
        oracle_numbers = re.findall(r"(?:lignes|rows).*?Oracle.*?(\d+)", report_content, re.IGNORECASE)
        
        if sqlite_numbers:
            rows_sqlite = max(int(num) for num in sqlite_numbers)
        if oracle_numbers:
            rows_oracle = max(int(num) for num in oracle_numbers)
    
    if rows_sqlite > 0 or rows_oracle > 0:
        stats["rows_sqlite"] = rows_sqlite
        stats["rows_oracle"] = rows_oracle
        
        if rows_sqlite > 0:
            stats["data_completion"] = min(100.0, (rows_oracle / rows_sqlite * 100))
        elif rows_oracle > 0:
            stats["data_completion"] = 100.0
    
    if stats["tables_sqlite"] > 0:
        tables_weight = 0.3
        data_weight = 0.7
        
        if stats["rows_sqlite"] > 0:
            stats["conversion_success"] = (
                tables_weight * stats["table_completion"] + 
                data_weight * stats["data_completion"]
            )
        else:
            stats["conversion_success"] = stats["table_completion"]
    elif stats["rows_sqlite"] > 0:
        stats["conversion_success"] = stats["data_completion"]
    
    stats["conversion_success"] = min(100.0, stats["conversion_success"])
    
    if stats["tables_sqlite"] > 0 and stats["rows_sqlite"] > 0:
        stats["overall_completion"] = (stats["table_completion"] + stats["data_completion"]) / 2
    elif stats["tables_sqlite"] > 0:
        stats["overall_completion"] = stats["table_completion"]
    elif stats["rows_sqlite"] > 0:
        stats["overall_completion"] = stats["data_completion"]
    
    logger.debug(f"Stats extraites: Tables SQLite={stats['tables_sqlite']}, Tables Oracle={stats['tables_oracle']}, "
                f"Lignes SQLite={stats['rows_sqlite']}, Lignes Oracle={stats['rows_oracle']}")
    logger.debug(f"Taux calculés: Tables={stats['table_completion']:.1f}%, "
                f"Données={stats['data_completion']:.1f}%, Conversion={stats['conversion_success']:.1f}%, Global={stats['overall_completion']:.1f}%")
    
    return stats

def validate_schema_with_output(
    sqlite_db_path: str,
    user_config: Dict[str, str],
    verbose: bool = False
) -> Tuple[bool, Dict[str, Any]]:
    """
    Valide le schéma et affiche les résultats avec formatage enrichi.
    
    Args:
        sqlite_db_path: Chemin vers le fichier SQLite source
        user_config: Configuration de connexion Oracle (user, password, dsn)
        verbose: Activer le mode verbose
        
    Returns:
        Tuple contenant (succès de la validation, statistiques de complétion)
    """
    username = user_config.get("user", "unknown")
    print_title(f"Validation de schéma: {sqlite_db_path} → Oracle ({username})")
    
    success, report_file, stats = validate_single_schema(sqlite_db_path, user_config, verbose)
    
    if success:
        if report_file:
            print_success_message(f"Validation terminée - rapport disponible: {report_file}")
        else:
            print_success_message("Validation terminée")
            
        if stats.get("rows_sqlite", 0) > 0:
            data_completion = stats.get("data_completion", 0)
            logger.info(f"Taux de complétion des données: {data_completion:.1f}% "
                       f"({stats.get('rows_oracle', 0):,}/{stats.get('rows_sqlite', 0):,} lignes)")
        
        overall = stats.get("overall_completion", 0)
        if overall > 0:
            completion_msg = f"Taux de complétion global: {overall:.1f}%"
            
            if overall < 90:
                from .rich_logging import print_warning_message
                print_warning_message(completion_msg + " (inférieur au seuil de 90%)")
            else:
                logger.info(completion_msg)
    else:
        print_error_message("Échec de la validation")
    
    return success, stats

def generate_batch_validation_report(
    validation_results: List[Dict[str, Any]], 
    failed_validations: List[Tuple[str, str]],
    output_directory: str,
    start_time: float,
    file_pattern: str
) -> Optional[str]:
    """
    Génère un rapport récapitulatif des validations par lots.
    
    Args:
        validation_results: Liste des configurations des validations réussies
        failed_validations: Liste des paires (chemin_fichier, raison_échec)
        output_directory: Répertoire de sortie pour le rapport
        start_time: Heure de début du processus (timestamp)
        file_pattern: Motif de fichiers utilisé pour la recherche
        
    Returns:
        Chemin vers le rapport généré ou None en cas d'échec
    """
    total_validations = len(validation_results) + len(failed_validations)
    if total_validations == 0:
        logger.warning("Aucune validation à inclure dans le rapport")
        return None
    
    end_time = time.time()
    duration = end_time - start_time
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_filename = f"validation_batch_report_{timestamp}.md"
    report_path = os.path.join(output_directory, report_filename)
    
    try:
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(f"# Rapport de validation par lots\n\n")
            f.write(f"_Généré le {datetime.datetime.now().strftime('%d/%m/%Y à %H:%M:%S')}_\n\n")
            
            f.write("## Résumé\n\n")
            f.write(f"- **Nombre total de bases validées**: {total_validations}\n")
            f.write(f"- **Validations réussies**: {len(validation_results)} ({(len(validation_results)/total_validations*100):.1f}%)\n")
            f.write(f"- **Validations échouées**: {len(failed_validations)} ({(len(failed_validations)/total_validations*100):.1f}%)\n")
            f.write(f"- **Durée totale**: {duration:.2f} secondes ({duration/60:.2f} minutes)\n")
            f.write(f"- **Motif de recherche**: `{file_pattern}`\n")
            f.write(f"- **Répertoire**: {output_directory}\n\n")
            
            if validation_results:
                f.write("## Bases validées avec succès\n\n")
                f.write("| # | Base de données | Utilisateur Oracle | Complétion Tables | Complétion Données | Convertissement Réussi | Complétion Globale |\n")
                f.write("|---|----------------|-------------------|-------------------|-------------------|----------------------|-------------------|\n")
                for i, config in enumerate(validation_results, 1):
                    db_path = config.get("source_db", "Inconnu")
                    db_name = os.path.basename(db_path)
                    user = config.get("user", "Inconnu")
                    
                    stats = config.get("stats", {})
                    table_completion = stats.get("table_completion", 0)
                    data_completion = stats.get("data_completion", 0)
                    conversion_success = stats.get("conversion_success", 0)
                    overall_completion = stats.get("overall_completion", 0)
                    
                    f.write(f"| {i} | {db_name} | {user} | {table_completion:.1f}% | {data_completion:.1f}% | {conversion_success:.1f}% | {overall_completion:.1f}% |\n")
                f.write("\n")
            
            if failed_validations:
                f.write("## Bases avec échec de validation\n\n")
                f.write("| # | Base de données | Raison de l'échec |\n")
                f.write("|---|----------------|-------------------|\n")
                for i, (db_path, reason) in enumerate(failed_validations, 1):
                    db_name = os.path.basename(db_path)
                    reason_short = reason if len(reason) < 80 else reason[:77] + "..."
                    f.write(f"| {i} | {db_name} | {reason_short} |\n")
                f.write("\n")
                
                f.write("### Détails des échecs\n\n")
                for i, (db_path, reason) in enumerate(failed_validations, 1):
                    db_name = os.path.basename(db_path)
                    f.write(f"#### {i}. {db_name}\n\n")
                    f.write(f"```\n{reason}\n```\n\n")
            
            if not failed_validations:
                f.write("Toutes les bases ont été validées avec succès! 🎉\n")
            else:
                f.write(f"Attention: Certaines bases ont échoué à la validation. Vérifiez les problèmes signalés.\n")
        
        logger.info(f"Rapport de validation par lots écrit dans {report_path}")
        return report_path
    
    except Exception as e:
        logger.error(f"Erreur lors de la génération du rapport de validation par lots: {str(e)}")
        return None

def generate_overall_status_report(
    validation_results: List[Dict[str, Any]],
    failed_validations: List[Tuple[str, str]],
    output_directory: str,
    duration: float,
    file_pattern: str
) -> Optional[str]:
    """
    Génère un rapport global simple contenant l'état de toutes les bases validées.
    
    Args:
        validation_results: Liste des configurations des validations réussies
        failed_validations: Liste des paires (chemin_fichier, raison_échec)
        output_directory: Répertoire de sortie pour le rapport
        duration: Durée totale du processus en secondes
        file_pattern: Motif de fichiers utilisé pour la recherche
        
    Returns:
        Chemin vers le rapport global ou None en cas d'échec
    """
    total_validations = len(validation_results) + len(failed_validations)
    if total_validations == 0:
        logger.warning("Aucune base à inclure dans le rapport global")
        return None
        
    report_path = os.path.join(output_directory, "overall_report.md")
    timestamp = datetime.datetime.now().strftime("%d/%m/%Y à %H:%M:%S")
    
    try:
        with open(report_path, 'w', encoding='utf-8') as f:
            # En-tête et résumé
            f.write(f"# État global des bases de données\n\n")
            f.write(f"_Rapport généré le {timestamp}_\n\n")
            
            success_rate = len(validation_results) / total_validations * 100 if total_validations else 0
            
            # Résumé sous forme de badges (meilleure lisibilité)
            f.write(f"![Total](<https://img.shields.io/badge/Total-{total_validations}-blue>)\n")
            f.write(f"![Succès](<https://img.shields.io/badge/Validées-{len(validation_results)}-{('green' if success_rate == 100 else 'yellow' if success_rate >= 80 else 'red')}>)\n")
            f.write(f"![Échecs](<https://img.shields.io/badge/Échouées-{len(failed_validations)}-{('green' if len(failed_validations) == 0 else 'red')}>)\n")
            f.write(f"![Durée](<https://img.shields.io/badge/Durée-{duration/60:.1f}%20min-blue>)\n\n")
            
            # Tableau de toutes les bases avec leur état
            f.write("## État détaillé de toutes les bases\n\n")
            f.write("| Base de données | État | Tables | Données | Conversion | Global |\n")
            f.write("|----------------|------|--------|---------|------------|--------|\n")
            
            # D'abord les bases validées avec succès
            high_quality_count = 0  # Compteur pour les bases avec complétion > 90%
            for config in validation_results:
                db_path = config.get("source_db", "Inconnu")
                db_name = os.path.basename(db_path)
                
                # Récupérer les statistiques de complétion
                stats = config.get("stats", {})
                
                # Debug - Afficher les valeurs brutes des statistiques
                rows_sqlite = stats.get("rows_sqlite", 0)
                rows_oracle = stats.get("rows_oracle", 0)
                logger.debug(f"Analyse des données pour {db_name}: SQLite={rows_sqlite}, Oracle={rows_oracle}")
                
                # Récupérer les statistiques avec valeurs par défaut explicites
                table_completion = stats.get("table_completion", 0.0)
                data_completion = stats.get("data_completion", 0.0)
                conversion_success = stats.get("conversion_success", 0.0)
                overall_completion = stats.get("overall_completion", 0.0)
                
                # Vérification de la cohérence des données
                if data_completion == 0.0 and stats.get("rows_sqlite", 0) > 0 and stats.get("rows_oracle", 0) > 0:
                    # Recalculer data_completion manuellement
                    data_completion = (stats.get("rows_oracle", 0) / stats.get("rows_sqlite", 0)) * 100
                    logger.warning(f"Recalcul du taux de complétion pour {db_name}: {data_completion:.1f}%")
                
                # Limiter les valeurs à l'intervalle [0, 100]
                table_completion = max(0.0, min(100.0, table_completion))
                data_completion = max(0.0, min(100.0, data_completion))
                conversion_success = max(0.0, min(100.0, conversion_success))
                overall_completion = max(0.0, min(100.0, overall_completion))
                
                # Déterminer le statut de complétion avec des icônes plus précises
                table_status = "✅" if table_completion >= 90 else "⚠️" if table_completion >= 70 else "❌"
                data_status = "✅" if data_completion >= 90 else "⚠️" if data_completion >= 70 else "❌"
                conversion_status = "✅" if conversion_success >= 90 else "⚠️" if conversion_success >= 70 else "❌"
                overall_status = "✅" if overall_completion >= 90 else "⚠️" if overall_completion >= 70 else "❌"
                
                # Formater les taux de complétion avec code couleur
                table_str = f"{table_completion:.1f}%"
                if table_completion < 90:
                    table_str = f"<span style='color:{('orange' if table_completion >= 70 else 'red')}'>{table_str}</span>"
                
                data_str = f"{data_completion:.1f}%"
                if data_completion < 90:
                    data_str = f"<span style='color:{('orange' if data_completion >= 70 else 'red')}'>{data_str}</span>"
                
                conversion_str = f"{conversion_success:.1f}%"
                if conversion_success < 90:
                    conversion_str = f"<span style='color:{('orange' if conversion_success >= 70 else 'red')}'>{conversion_str}</span>"
                
                overall_str = f"{overall_completion:.1f}%"
                if overall_completion < 90:
                    overall_str = f"<span style='color:{('orange' if overall_completion >= 70 else 'red')}'>{overall_str}</span>"
                
                # État global de la base
                db_status = "✅" if overall_completion >= 90 else "⚠️" if overall_completion >= 70 else "⛔"
                
                # Incrémenter le compteur si la complétion globale est élevée
                if overall_completion >= 90:
                    high_quality_count += 1
                
                f.write(f"| {db_name} | {db_status} | {table_str} {table_status} | {data_str} {data_status} | {conversion_str} {conversion_status} | {overall_str} {overall_status} |\n")
            
            # Ensuite les bases ayant échoué
            for db_path, reason in failed_validations:
                db_name = os.path.basename(db_path)
                short_reason = reason[:30] + "..." if len(reason) > 30 else reason
                
                # Pour les bases en échec, on met des cases vides pour les taux de complétion
                f.write(f"| {db_name} | ❌ | - | - | - | - |\n")
            
            # Ajouter une ligne de séparation et de total pour les bases avec un taux élevé
            if validation_results:
                success_percentage = (high_quality_count / len(validation_results) * 100) if validation_results else 0
                f.write("|----------------|------|--------|---------|------------|--------|\n")
                
                total_status = "✅" if success_percentage >= 90 else "⚠️" if success_percentage >= 70 else "❌"
                total_str = f"{high_quality_count}/{len(validation_results)} bases avec complétion ≥ 90% ({success_percentage:.1f}%)"
                if success_percentage < 90:
                    total_str = f"<span style='color:{('orange' if success_percentage >= 70 else 'red')}'>{total_str}</span>"
                
                f.write(f"| **TOTAL** | {total_status} | **-** | **-** | **-** | {total_str} |\n")
            
            f.write("\n")
            
            # Calcul des statistiques globales si au moins une base validée
            if validation_results:
                # Calculer les moyennes correctement en ignorant les valeurs invalides
                valid_table_completions = [config.get("stats", {}).get("table_completion", 0.0) for config in validation_results if config.get("stats", {}).get("table_completion", 0.0) > 0]
                valid_data_completions = [config.get("stats", {}).get("data_completion", 0.0) for config in validation_results if config.get("stats", {}).get("data_completion", 0.0) > 0]
                valid_conversion_completions = [config.get("stats", {}).get("conversion_success", 0.0) for config in validation_results if config.get("stats", {}).get("conversion_success", 0.0) > 0]
                valid_overall_completions = [config.get("stats", {}).get("overall_completion", 0.0) for config in validation_results if config.get("stats", {}).get("overall_completion", 0.0) > 0]
                
                avg_table_completion = sum(valid_table_completions) / len(valid_table_completions) if valid_table_completions else 0.0
                avg_data_completion = sum(valid_data_completions) / len(valid_data_completions) if valid_data_completions else 0.0
                avg_conversion_completion = sum(valid_conversion_completions) / len(valid_conversion_completions) if valid_conversion_completions else 0.0
                avg_overall_completion = sum(valid_overall_completions) / len(valid_overall_completions) if valid_overall_completions else 0.0
                
                # Log pour débogage
                logger.debug(f"Calcul des moyennes - Tables: {avg_table_completion:.1f}%, Données: {avg_data_completion:.1f}%, "
                            f"Conversion: {avg_conversion_completion:.1f}%, Global: {avg_overall_completion:.1f}%")
                
                f.write("## Statistiques globales\n\n")
                
                status_color = 'green' if avg_overall_completion >= 90 else 'yellow' if avg_overall_completion >= 70 else 'red'
                overall_status = 'success' if avg_overall_completion >= 90 else 'warning' if avg_overall_completion >= 70 else 'critical'
                
                # Barre de complétion globale
                f.write(f"![Complétion](<https://img.shields.io/badge/Taux%20global-{avg_overall_completion:.1f}%25-{status_color}>)\n\n")
                
                # Tableau de statistiques
                f.write("| Métrique | Valeur | État |\n")
                f.write("|----------|--------|------|\n")
                
                # Ajout d'indicateurs plus précis
                table_status_icon = "✅" if avg_table_completion >= 90 else "⚠️" if avg_table_completion >= 70 else "❌"
                data_status_icon = "✅" if avg_data_completion >= 90 else "⚠️" if avg_data_completion >= 70 else "❌"
                conversion_status_icon = "✅" if avg_conversion_completion >= 90 else "⚠️" if avg_conversion_completion >= 70 else "❌"
                overall_status_icon = "✅" if avg_overall_completion >= 90 else "⚠️" if avg_overall_completion >= 70 else "❌"
                
                # Ajout de couleurs pour une meilleure visibilité
                table_val = f"{avg_table_completion:.1f}%"
                if avg_table_completion < 90:
                    table_val = f"<span style='color:{('orange' if avg_table_completion >= 70 else 'red')}'>{table_val}</span>"
                
                data_val = f"{avg_data_completion:.1f}%"
                if avg_data_completion < 90:
                    data_val = f"<span style='color:{('orange' if avg_data_completion >= 70 else 'red')}'>{data_val}</span>"
                
                conversion_val = f"{avg_conversion_completion:.1f}%"
                if avg_conversion_completion < 90:
                    conversion_val = f"<span style='color:{('orange' if avg_conversion_completion >= 70 else 'red')}'>{conversion_val}</span>"
                
                overall_val = f"**{avg_overall_completion:.1f}%**"
                if avg_overall_completion < 90:
                    overall_val = f"<span style='color:{('orange' if avg_overall_completion >= 70 else 'red')}'>{overall_val}</span>"
                
                f.write(f"| Tables | {table_val} | {table_status_icon} |\n")
                f.write(f"| Données | {data_val} | {data_status_icon} |\n")
                f.write(f"| Conversion | {conversion_val} | {conversion_status_icon} |\n")
                f.write(f"| **Global** | {overall_val} | {overall_status_icon} |\n")
                
                # Ajouter une légende pour les icônes
                f.write("\n### Légende\n\n")
                f.write("- ✅ : Complétion ≥ 90% (Bon)\n")
                f.write("- ⚠️ : Complétion entre 70% et 90% (Attention)\n")
                f.write("- ❌ : Complétion < 70% (Problème)\n")
                f.write("- ⛔ : Échec complet de la validation\n")
                
                # Ajouter l'indicateur de qualité globale
                f.write(f"\n**Qualité globale**: {high_quality_count}/{len(validation_results)} bases ({success_percentage:.1f}%) ont un taux de complétion global ≥ 90%")
                if success_percentage < 90:
                    f.write(" ⚠️")
                elif success_percentage == 100:
                    f.write(" 🎉")
            
            # Lien vers le rapport détaillé
            f.write("\n## Informations complémentaires\n\n")
            f.write("Pour plus de détails, consultez le rapport de validation complet.\n")
            
        logger.info(f"Rapport global d'état écrit dans {report_path}")
        return report_path
    
    except Exception as e:
        logger.error(f"Erreur lors de la génération du rapport global d'état: {str(e)}")
        return None

def process_batch_validation(
    oracle_config: Dict[str, str],
    sqlite_dir: str,
    file_pattern: str = "*.sqlite",
    use_admin_user: bool = False,
    new_username: Optional[str] = None,
    new_password: Optional[str] = None, 
    continue_on_error: bool = False,
    verbose: bool = False
) -> List[Dict[str, str]]:
    """
    Exécute la validation du schéma en mode batch sur plusieurs bases de données.
    
    Args:
        oracle_config: Configuration Oracle administrateur
        sqlite_dir: Répertoire contenant les fichiers SQLite
        file_pattern: Motif de fichiers à traiter
        use_admin_user: Utiliser l'utilisateur administrateur au lieu de créer un utilisateur
        new_username: Nom du nouvel utilisateur Oracle (si spécifié)
        new_password: Mot de passe du nouvel utilisateur Oracle (si spécifié)
        continue_on_error: Continuer le traitement même en cas d'erreur
        verbose: Activer le mode verbose
        
    Returns:
        Liste des configurations de connexion des bases validées avec succès
    """
    start_time = time.time()
    
    successful_validations = []
    failed_validations = []
    
    file_pattern_path = os.path.join(sqlite_dir, file_pattern)
    logger.info(f"Recherche des fichiers SQLite dans {sqlite_dir} avec le motif {file_pattern}...")
    
    sqlite_files = glob.glob(file_pattern_path)
    
    if not sqlite_files:
        logger.warning(f"Aucun fichier SQLite trouvé avec le motif {file_pattern} dans {sqlite_dir}")
        return []
    
    print_title(f"Validation en lot de {len(sqlite_files)} bases de données SQLite")
    for i, file_path in enumerate(sqlite_files):
        logger.info(f"[{i+1}/{len(sqlite_files)}] {os.path.basename(file_path)}")
    
    for i, sqlite_db_path in enumerate(sqlite_files):
        db_name = os.path.basename(sqlite_db_path)
        print_title(f"[{i+1}/{len(sqlite_files)}] Validation de {db_name}")
        
        if use_admin_user:
            target_username = oracle_config["user"]
            target_password = oracle_config["password"]
        else:
            oracle_username = get_oracle_username_from_filepath(sqlite_db_path)
            
            target_username = new_username if new_username else oracle_username
            target_password = new_password if new_password else target_username
            
            logger.info(f"Utilisateur Oracle sélectionné pour la validation: {target_username}")
        
        user_config = {
            "user": target_username,
            "password": target_password,
            "dsn": oracle_config["dsn"],
            "source_db": sqlite_db_path
        }
        
        try:
            success, stats = validate_schema_with_output(sqlite_db_path, user_config, verbose)
            
            if success:
                rows_sqlite = stats.get("rows_sqlite", 0)
                rows_oracle = stats.get("rows_oracle", 0)
                
                if rows_sqlite > 0:
                    data_completion = min(100.0, (rows_oracle / rows_sqlite) * 100)
                    
                    current_data_completion = stats.get("data_completion", 0.0)
                    if abs(data_completion - current_data_completion) > 0.5:
                        logger.warning(f"Correction du taux de complétion pour {db_name}: "
                                      f"{current_data_completion:.1f}% → {data_completion:.1f}%")
                        stats["data_completion"] = data_completion
                
                    table_completion = stats.get("table_completion", 0.0)
                    if table_completion > 0:
                        stats["overall_completion"] = (table_completion + data_completion) / 2
                
                logger.debug(f"Statistiques finales pour {db_name}: "
                           f"Tables={stats.get('table_completion', 0):.1f}%, "
                           f"Données={stats.get('data_completion', 0):.1f}%, "
                           f"Conversion={stats.get('conversion_success', 0):.1f}%, "
                           f"Global={stats.get('overall_completion', 0):.1f}%")
                
                user_config["stats"] = stats
                successful_validations.append(user_config)
            else:
                failed_validations.append((sqlite_db_path, "Échec de la validation du schéma sans erreur spécifique"))
                
                if not continue_on_error:
                    logger.error("Arrêt du traitement par lots suite à une erreur")
                    break
                
        except Exception as e:
            error_message = str(e)
            print_error_message(f"Erreur lors de la validation de {db_name}: {error_message}")
            if verbose:
                print_exception(e)
            
            failed_validations.append((sqlite_db_path, error_message))
            
            if not continue_on_error:
                logger.error("Arrêt du traitement par lots suite à une erreur")
                break
        
        if i < len(sqlite_files) - 1:
            time.sleep(0.5)
    
    stats_log_path = os.path.join(sqlite_dir, "validation_stats.log")
    try:
        with open(stats_log_path, 'w', encoding='utf-8') as stats_log:
            stats_log.write("# Statistiques de validation\n\n")
            stats_log.write("| Base | Tables SQLite | Tables Oracle | Lignes SQLite | Lignes Oracle | Complétion Tables | Complétion Données | Conversion Réussie | Complétion Globale |\n")
            stats_log.write("|------|--------------|---------------|---------------|---------------|-------------------|-------------------|-------------------|-------------------|\n")
            
            for config in successful_validations:
                db_name = os.path.basename(config.get("source_db", "Inconnu"))
                stats = config.get("stats", {})
                tables_sqlite = stats.get("tables_sqlite", 0)
                tables_oracle = stats.get("tables_oracle", 0)
                rows_sqlite = stats.get("rows_sqlite", 0)
                rows_oracle = stats.get("rows_oracle", 0)
                table_completion = stats.get("table_completion", 0.0)
                data_completion = stats.get("data_completion", 0.0)
                conversion_success = stats.get("conversion_success", 0.0)
                overall_completion = stats.get("overall_completion", 0.0)
                
                stats_log.write(f"| {db_name} | {tables_sqlite} | {tables_oracle} | {rows_sqlite:,} | {rows_oracle:,} | {table_completion:.1f}% | {data_completion:.1f}% | {conversion_success:.1f}% | {overall_completion:.1f}% |\n")
        
        logger.info(f"Log détaillé des statistiques écrit dans {stats_log_path}")
    except Exception as e:
        logger.error(f"Impossible d'écrire le fichier de log des statistiques: {str(e)}")
    
    report_path = generate_batch_validation_report(
        validation_results=successful_validations,
        failed_validations=failed_validations,
        output_directory=sqlite_dir,
        start_time=start_time,
        file_pattern=file_pattern
    )
    
    if report_path:
        print_success_message(f"Rapport de synthèse disponible: {report_path}")
    
    overall_report_path = generate_overall_status_report(
        validation_results=successful_validations,
        failed_validations=failed_validations,
        output_directory=sqlite_dir,
        duration=time.time() - start_time,
        file_pattern=file_pattern
    )
    
    if overall_report_path:
        print_success_message(f"Rapport global disponible: {overall_report_path}")
    
    return successful_validations
