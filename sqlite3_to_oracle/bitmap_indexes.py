"""
Module pour ajouter des index bitmap entre attributs dans Oracle.

Les index bitmap sont très efficaces pour:
- Les colonnes à faible cardinalité (peu de valeurs distinctes)
- Les requêtes analytiques avec plusieurs critères de filtrage
- Les environnements principalement en lecture
"""

import re
import oracledb
from typing import Dict, List, Tuple, Set, Optional
import logging
from . import logger

def identify_bitmap_candidates(connection: oracledb.Connection, 
                              table_name: str, 
                              max_distinct_ratio: float = 0.1, 
                              min_rows: int = 100) -> List[str]:
    """
    Identifie les colonnes candidates pour des index bitmap.
    
    Args:
        connection: Connexion Oracle
        table_name: Nom de la table à analyser
        max_distinct_ratio: Ratio maximal (valeurs distinctes / total) pour éligibilité
        min_rows: Nombre minimal de lignes pour considérer une table
        
    Returns:
        Liste des noms de colonnes candidates pour un index bitmap
    """
    cursor = connection.cursor()
    candidates = []
    
    try:
        # Vérifier si la table contient suffisamment de lignes
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        if row_count < min_rows:
            logger.debug(f"Table {table_name} contient seulement {row_count} lignes, ignorée pour bitmap index")
            return []
        
        # Obtenir toutes les colonnes de la table
        cursor.execute(f"SELECT column_name, data_type, nullable FROM user_tab_columns WHERE table_name = '{table_name}'")
        columns = cursor.fetchall()
        
        # Récupérer les statistiques de colonnes si disponibles
        column_stats = {}
        try:
            cursor.execute(f"""
                SELECT column_name, num_distinct, density
                FROM user_tab_col_statistics
                WHERE table_name = '{table_name}'
            """)
            for col_name, num_distinct, density in cursor.fetchall():
                column_stats[col_name] = {
                    'num_distinct': num_distinct,
                    'density': density
                }
        except:
            # La vue des statistiques n'est peut-être pas accessible pour cet utilisateur
            pass
        
        # Privilégier certains types de colonnes préférentiels pour les bitmap index
        priority_types = ['CHAR', 'VARCHAR2', 'NUMBER', 'DATE', 'TIMESTAMP', 'INTEGER']
        priority_columns = []
        
        for column_name, data_type, nullable in columns:
            # Exclure les types de données inappropriés pour les index bitmap
            if data_type.startswith('LONG') or data_type.startswith('LOB') or data_type == 'BLOB' or data_type == 'CLOB':
                continue
                
            # Ajouter aux colonnes prioritaires pour analyse approfondie
            if any(ptype in data_type.upper() for ptype in priority_types):
                priority_columns.append((column_name, data_type, nullable))
        
        # Analyser les colonnes prioritaires
        for column_name, data_type, nullable in priority_columns:
            # Utiliser les statistiques existantes si disponibles
            if column_name in column_stats:
                stats = column_stats[column_name]
                distinct_count = stats['num_distinct']
                distinct_ratio = distinct_count / row_count if row_count > 0 else 1
                
                if distinct_ratio <= max_distinct_ratio:
                    candidates.append(column_name)
                    logger.debug(f"Colonne {column_name} dans {table_name} identifiée pour bitmap index "
                               f"(ratio={distinct_ratio:.4f}, valeurs_distinctes={distinct_count})")
                continue
                
            # Vérifier le ratio de distinction
            try:
                # Optimisation: Limiter le comptage des valeurs distinctes 
                # pour éviter de scanner toute la table sur les grands ensembles de données
                if row_count > 10000:
                    cursor.execute(f"""
                        SELECT COUNT(DISTINCT {column_name}) 
                        FROM (SELECT {column_name} FROM {table_name} SAMPLE(10) SEED(1))
                    """)
                    sample_distinct = cursor.fetchone()[0]
                    sample_ratio = sample_distinct / (row_count * 0.1)
                    
                    # Si même l'échantillon montre une cardinalité élevée, ignorer
                    if sample_ratio > max_distinct_ratio * 1.5:
                        logger.debug(f"Colonne {column_name} rejetée sur échantillon (ratio={sample_ratio:.4f})")
                        continue
                
                # Effectuer le comptage complet si nécessaire
                cursor.execute(f"SELECT COUNT(DISTINCT {column_name}) FROM {table_name}")
                distinct_count = cursor.fetchone()[0]
                
                if distinct_count == 0:
                    continue
                    
                distinct_ratio = distinct_count / row_count
                
                # Ajouter des critères supplémentaires pour de meilleurs index bitmap
                if distinct_ratio <= max_distinct_ratio:
                    # Vérifier si la colonne est souvent utilisée dans des clauses WHERE
                    # (approximatif - on privilégie les colonnes utilisées dans des contraintes)
                    is_in_constraint = False
                    try:
                        cursor.execute(f"""
                            SELECT COUNT(*) FROM user_constraints c, user_cons_columns cc
                            WHERE c.constraint_name = cc.constraint_name
                            AND c.table_name = '{table_name}'
                            AND cc.column_name = '{column_name}'
                        """)
                        in_constraint_count = cursor.fetchone()[0]
                        is_in_constraint = in_constraint_count > 0
                    except:
                        pass
                    
                    # Colonnes nullable avec peu de valeurs sont d'excellents candidats
                    is_nullable = nullable == 'Y'
                    
                    # Ajouter avec information supplémentaire pour le débogage
                    candidates.append(column_name)
                    logger.debug(f"Colonne {column_name} dans {table_name} identifiée pour bitmap index "
                                f"(ratio={distinct_ratio:.4f}, valeurs_distinctes={distinct_count}, "
                                f"constraint={is_in_constraint}, nullable={is_nullable})")
            except oracledb.DatabaseError as e:
                # Ignorer les erreurs pour cette colonne
                logger.debug(f"Erreur lors de l'analyse de {column_name}: {str(e)}")
                continue
    
    except oracledb.DatabaseError as e:
        logger.warning(f"Erreur lors de l'analyse de la table {table_name} pour bitmap indexes: {str(e)}")
    finally:
        cursor.close()
        
    return candidates

def identify_correlated_columns(connection: oracledb.Connection, 
                               table_name: str, 
                               candidates: List[str], 
                               correlation_threshold: float = 0.5) -> List[Tuple[str, str]]:
    """
    Identifie les paires de colonnes qui sont corrélées et bonnes pour des index bitmap combinés.
    
    Args:
        connection: Connexion Oracle
        table_name: Nom de la table
        candidates: Liste des colonnes candidates
        correlation_threshold: Seuil de corrélation minimum
        
    Returns:
        Liste de tuples (colonne1, colonne2) à indexer ensemble
    """
    cursor = connection.cursor()
    correlated_pairs = []
    
    try:
        for i, col1 in enumerate(candidates):
            for col2 in candidates[i+1:]:
                # Vérifier s'il existe des requêtes qui utilisent fréquemment ces deux colonnes ensemble
                # Ceci est une approximation basée sur les statistiques système
                try:
                    cursor.execute(f"""
                    SELECT COUNT(*) FROM {table_name} t1
                    WHERE EXISTS (
                        SELECT 1 FROM {table_name} t2
                        WHERE t2.{col1} = t1.{col1} AND t2.{col2} = t1.{col2}
                    )
                    """)
                    
                    correlation_count = cursor.fetchone()[0]
                    
                    # Obtenir le nombre total de lignes
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    total_rows = cursor.fetchone()[0]
                    
                    if total_rows > 0:
                        correlation_ratio = correlation_count / total_rows
                        
                        if correlation_ratio >= correlation_threshold:
                            correlated_pairs.append((col1, col2))
                            logger.debug(f"Corrélation détectée entre {col1} et {col2} dans {table_name} "
                                        f"(ratio={correlation_ratio:.4f})")
                
                except oracledb.DatabaseError as e:
                    logger.debug(f"Erreur lors de l'analyse de corrélation entre {col1} et {col2}: {str(e)}")
                    continue
    
    except oracledb.DatabaseError as e:
        logger.warning(f"Erreur lors de l'analyse des corrélations dans {table_name}: {str(e)}")
    finally:
        cursor.close()
        
    return correlated_pairs

def create_bitmap_indexes(connection: oracledb.Connection, 
                         table_name: str, 
                         auto_identify: bool = True, 
                         specific_columns: Optional[List[str]] = None,
                         correlated_pairs: Optional[List[Tuple[str, str]]] = None) -> Dict[str, bool]:
    """
    Crée des index bitmap sur les colonnes spécifiées ou identifiées automatiquement.
    
    Args:
        connection: Connexion Oracle
        table_name: Nom de la table
        auto_identify: Si True, identifie automatiquement les colonnes appropriées
        specific_columns: Liste spécifique de colonnes pour créer des index bitmap
        correlated_pairs: Paires de colonnes pour créer des index bitmap composites
        
    Returns:
        Dictionnaire des index créés avec leur statut
    """
    cursor = connection.cursor()
    results = {}
    
    columns_to_index = specific_columns or []
    pairs_to_index = correlated_pairs or []
    
    try:
        if auto_identify:
            # Identifier automatiquement les colonnes à faible cardinalité
            candidates = identify_bitmap_candidates(connection, table_name)
            columns_to_index.extend([c for c in candidates if c not in columns_to_index])
            
            # Identifier les paires de colonnes corrélées
            if len(candidates) > 1:
                pairs_to_index.extend(identify_correlated_columns(connection, table_name, candidates))
        
        # Créer des index bitmap simples
        for column in columns_to_index:
            index_name = f"BMX_{table_name}_{column}".upper()
            
            try:
                # Vérifier si l'index existe déjà
                cursor.execute(f"SELECT COUNT(*) FROM user_indexes WHERE index_name = '{index_name}'")
                if cursor.fetchone()[0] > 0:
                    logger.info(f"Index bitmap {index_name} existe déjà")
                    results[index_name] = True
                    continue
                
                # Créer l'index bitmap
                create_stmt = f"CREATE BITMAP INDEX {index_name} ON {table_name}({column})"
                cursor.execute(create_stmt)
                connection.commit()
                
                logger.info(f"Index bitmap créé avec succès: {index_name}")
                results[index_name] = True
                
            except oracledb.DatabaseError as e:
                logger.error(f"Erreur lors de la création de l'index bitmap {index_name}: {str(e)}")
                results[index_name] = False
        
        # Créer des index bitmap composites pour les paires corrélées
        for col1, col2 in pairs_to_index:
            index_name = f"BMX_{table_name}_{col1}_{col2}".upper()
            
            try:
                # Vérifier si l'index existe déjà
                cursor.execute(f"SELECT COUNT(*) FROM user_indexes WHERE index_name = '{index_name}'")
                if cursor.fetchone()[0] > 0:
                    logger.info(f"Index bitmap {index_name} existe déjà")
                    results[index_name] = True
                    continue
                
                # Créer l'index bitmap composite
                create_stmt = f"CREATE BITMAP INDEX {index_name} ON {table_name}({col1}, {col2})"
                cursor.execute(create_stmt)
                connection.commit()
                
                logger.info(f"Index bitmap composite créé avec succès: {index_name}")
                results[index_name] = True
                
            except oracledb.DatabaseError as e:
                logger.error(f"Erreur lors de la création de l'index bitmap composite {index_name}: {str(e)}")
                results[index_name] = False
    
    except Exception as e:
        logger.error(f"Erreur lors de la création des index bitmap: {str(e)}")
    finally:
        cursor.close()
        
    return results

def add_bitmap_indexes_to_database(config: Dict[str, str], 
                                  tables: Optional[List[str]] = None, 
                                  auto_detect: bool = True,
                                  max_distinct_ratio: float = 0.1,
                                  exclude_tables: Optional[List[str]] = None) -> Dict[str, Dict[str, bool]]:
    """
    Ajoute des index bitmap à la base de données.
    
    Args:
        config: Configuration Oracle (user, password, dsn)
        tables: Liste des tables pour ajouter des index bitmap (si None, utilise toutes les tables)
        auto_detect: Si True, détecte automatiquement les colonnes appropriées
        max_distinct_ratio: Ratio maximal pour considérer une colonne comme candidate (0.1 = 10%)
        exclude_tables: Liste de tables à exclure du traitement
        
    Returns:
        Dictionnaire des résultats par table
    """
    try:
        connection = oracledb.connect(
            user=config["user"],
            password=config["password"],
            dsn=config["dsn"]
        )
        
        results = {}
        
        cursor = connection.cursor()
        
        # Si aucune table n'est spécifiée, prendre toutes les tables de l'utilisateur
        if tables is None:
            cursor.execute("SELECT table_name FROM user_tables")
            tables = [row[0] for row in cursor.fetchall()]
        
        # Filtrer les tables exclues
        if exclude_tables:
            exclude_tables = [t.upper() for t in exclude_tables]
            tables = [t for t in tables if t.upper() not in exclude_tables]
        
        logger.info(f"Ajout d'index bitmap pour {len(tables)} tables...")
        
        # Optimiser la création d'index en obtenant des statistiques sur toutes les tables
        table_stats = {}
        try:
            cursor.execute("""
                SELECT table_name, num_rows
                FROM user_tables
                WHERE table_name IN (
                    SELECT table_name FROM user_tables 
                    WHERE table_name IN ({})
                )
            """.format(','.join([f"'{t}'" for t in tables])))
            
            for table_name, num_rows in cursor.fetchall():
                table_stats[table_name] = {'num_rows': num_rows or 0}
        except:
            pass
        
        # Trier les tables par taille pour traiter d'abord les plus petites
        # Cela permet de voir des résultats plus rapidement
        tables_with_size = [(t, table_stats.get(t, {}).get('num_rows', 0)) for t in tables]
        sorted_tables = [t for t, _ in sorted(tables_with_size, key=lambda x: x[1])]
        
        # Traiter chaque table
        for table in sorted_tables:
            size_info = table_stats.get(table, {}).get('num_rows', 'inconnu')
            logger.info(f"Analyse de la table {table} (lignes: {size_info}) pour l'ajout d'index bitmap...")
            table_results = create_bitmap_indexes(connection, table, auto_identify=auto_detect)
            results[table] = table_results
            
            # Afficher un résumé pour cette table
            success_count = sum(1 for status in table_results.values() if status)
            if table_results:
                logger.info(f"Table {table}: {success_count}/{len(table_results)} index bitmap créés avec succès")
        
        cursor.close()
        connection.close()
        
        # Afficher un résumé global
        total_indexes = sum(len(indexes) for indexes in results.values())
        total_success = sum(sum(1 for status in indexes.values() if status) for indexes in results.values())
        
        logger.info(f"Résumé global: {total_success}/{total_indexes} index bitmap créés avec succès sur {len(tables)} tables")
        
        return results
        
    except oracledb.DatabaseError as e:
        logger.error(f"Erreur de base de données lors de l'ajout des index bitmap: {str(e)}")
        return {}
    except Exception as e:
        logger.error(f"Erreur lors de l'ajout des index bitmap: {str(e)}")
        return {}

def generate_bitmap_index_report(results: Dict[str, Dict[str, bool]], include_details: bool = True) -> str:
    """
    Génère un rapport sur les index bitmap créés.
    
    Args:
        results: Résultats de la création d'index bitmap
        include_details: Si True, inclut les détails par table
        
    Returns:
        Rapport formaté
    """
    report = []
    report.append("# Rapport des index bitmap")
    report.append("\n## Résumé")
    
    total_tables = len(results)
    total_indexes = sum(len(indexes) for indexes in results.values())
    total_success = sum(sum(1 for status in indexes.values() if status) for indexes in results.values())
    
    # Aucun index créé
    if total_indexes == 0:
        report.append("\nAucun index bitmap n'a été créé. Cela peut être dû à l'une des raisons suivantes:")
        report.append("- Les tables ne contiennent pas assez de lignes")
        report.append("- Aucune colonne ne présente un faible ratio de valeurs distinctes")
        report.append("- Les utilisateurs n'ont pas les privilèges nécessaires pour créer des index")
        return "\n".join(report)
    
    report.append(f"\nTables analysées: {total_tables}")
    report.append(f"Index bitmap créés: {total_success}/{total_indexes} ({(total_success/total_indexes*100):.1f}% de réussite)")
    
    # Répartition par type d'index
    composite_indexes = sum(1 for table, indexes in results.items() 
                          for idx_name in indexes.keys() 
                          if sum(1 for c in idx_name.split('_') if c not in ['BMX', table]) > 1)
    single_indexes = total_indexes - composite_indexes
    
    report.append(f"Index simples: {single_indexes}")
    report.append(f"Index composites: {composite_indexes}")
    
    # Tables ayant le plus d'index
    if total_tables > 1:
        tables_by_index_count = sorted(
            [(table, len(indexes)) for table, indexes in results.items()],
            key=lambda x: x[1], reverse=True
        )
        
        report.append("\n### Tables avec le plus d'index bitmap")
        for table, count in tables_by_index_count[:5]:  # Top 5
            if count > 0:
                success_count = sum(1 for status in results[table].values() if status)
                report.append(f"- {table}: {success_count}/{count} index")
    
    if include_details:
        report.append("\n## Détails par table")
        
        for table, indexes in results.items():
            if not indexes:
                continue
                
            success_count = sum(1 for status in indexes.values() if status)
            report.append(f"\n### Table: {table}")
            report.append(f"Index créés: {success_count}/{len(indexes)}")
            
            if indexes:
                report.append("\n| Nom de l'index | Type | Statut |")
                report.append("| ------------- | ---- | ------ |")
                
                for index_name, status in indexes.items():
                    # Déterminer le type d'index (simple ou composite)
                    index_type = "Composite" if sum(1 for c in index_name.split('_') if c not in ['BMX', table]) > 1 else "Simple"
                    status_text = "✅ Succès" if status else "❌ Échec"
                    report.append(f"| {index_name} | {index_type} | {status_text} |")
    
    report.append("\n## Avantages des index bitmap")
    report.append("\nLes index bitmap créés offrent les avantages suivants:")
    report.append("- Amélioration des performances des requêtes analytiques et de reporting")
    report.append("- Optimisation des requêtes avec multiples conditions de filtrage")
    report.append("- Réduction de l'utilisation des ressources système pour les requêtes complexes")
    report.append("- Particulièrement efficaces pour les colonnes à faible cardinalité")
    
    report.append("\n## Bonnes pratiques")
    report.append("\nPour optimiser l'utilisation des index bitmap:")
    report.append("- Utilisez-les davantage pour les environnements de reporting/lecture plutôt que OLTP")
    report.append("- Construisez des requêtes qui filtrent sur plusieurs colonnes indexées en bitmap")
    report.append("- Recalculez les statistiques après le chargement de données volumineuses")
    report.append("- Évitez les index bitmap sur les tables fréquemment modifiées")
    
    return "\n".join(report)

def add_bitmap_indexes_to_validation_workflow(connection: oracledb.Connection, 
                                             report_content: str,
                                             auto_detect: bool = True) -> str:
    """
    Ajoute une section sur les index bitmap au rapport de validation.
    
    Args:
        connection: Connexion Oracle
        report_content: Contenu du rapport de validation existant
        auto_detect: Si True, détecte automatiquement les candidats pour les index bitmap
        
    Returns:
        Rapport mis à jour avec la section sur les index bitmap
    """
    cursor = connection.cursor()
    
    try:
        # Extraire les tables validées du rapport
        tables = []
        table_pattern = re.compile(r"Table: ([^\n]+)")
        for match in table_pattern.finditer(report_content):
            table_name = match.group(1).strip()
            if table_name and table_name not in tables:
                tables.append(table_name)
        
        logger.info(f"Analyse de {len(tables)} tables pour les index bitmap...")
        
        # Créer les index bitmap pour ces tables
        bitmap_results = {}
        for table in tables:
            bitmap_results[table] = create_bitmap_indexes(connection, table, auto_identify=auto_detect)
        
        # Générer le rapport des index bitmap
        bitmap_report = generate_bitmap_index_report(bitmap_results)
        
        # Ajouter le rapport des index bitmap au rapport existant
        updated_report = report_content + "\n\n" + bitmap_report
        
        return updated_report
        
    except Exception as e:
        logger.error(f"Erreur lors de l'ajout des index bitmap au rapport: {str(e)}")
        return report_content
    finally:
        cursor.close()

def add_bitmap_indexes_after_validation(sqlite_path: str, oracle_config: Dict[str, str]) -> Dict[str, Dict[str, bool]]:
    """
    Ajoute des index bitmap après la validation du schéma.
    
    Args:
        sqlite_path: Chemin vers le fichier SQLite source
        oracle_config: Configuration Oracle
        
    Returns:
        Résultats de la création des index
    """
    try:
        logger.info(f"Création d'index bitmap pour optimiser les performances analytiques...")
        
        # Se connecter à Oracle
        connection = oracledb.connect(
            user=oracle_config["user"],
            password=oracle_config["password"],
            dsn=oracle_config["dsn"]
        )
        
        # Obtenir toutes les tables de la base
        cursor = connection.cursor()
        cursor.execute("SELECT table_name FROM user_tables")
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        
        # Ajouter des index bitmap pour chaque table
        results = {}
        for table in tables:
            logger.info(f"Analyse de {table} pour les index bitmap...")
            table_results = create_bitmap_indexes(connection, table, auto_identify=True)
            results[table] = table_results
        
        connection.close()
        
        # Générer un rapport
        total_indexes = sum(len(indexes) for indexes in results.values())
        successful_indexes = sum(sum(1 for status in indexes.values() if status) for indexes in results.values())
        
        if total_indexes > 0:
            logger.info(f"Index bitmap créés: {successful_indexes}/{total_indexes}")
            logger.info(f"Les index bitmap améliorent les performances des requêtes analytiques.")
        else:
            logger.info("Aucun index bitmap n'a été créé (pas de colonnes éligibles).")
        
        return results
        
    except Exception as e:
        logger.error(f"Erreur lors de la création des index bitmap: {str(e)}")
        return {}

# Point d'entrée pour l'exécution en tant que script indépendant
if __name__ == "__main__":
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description="Ajouter des index bitmap à une base de données Oracle")
    parser.add_argument("--config", required=True, help="Fichier de configuration Oracle JSON")
    parser.add_argument("--tables", help="Liste des tables séparées par des virgules (si non spécifié, utilise toutes les tables)")
    parser.add_argument("--report", help="Fichier de sortie pour le rapport")
    parser.add_argument("--no-auto-detect", action="store_true", help="Désactiver la détection automatique des colonnes appropriées")
    
    args = parser.parse_args()
    
    # Configurer le logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Charger la configuration
    with open(args.config, 'r') as f:
        config = json.load(f)
    
    # Traiter les tables
    tables = args.tables.split(',') if args.tables else None
    
    # Ajouter les index bitmap
    results = add_bitmap_indexes_to_database(config, tables, not args.no_auto_detect)
    
    # Générer le rapport
    report = generate_bitmap_index_report(results)
    
    # Sauvegarder le rapport si demandé
    if args.report:
        with open(args.report, 'w') as f:
            f.write(report)
        print(f"Rapport sauvegardé dans {args.report}")
    else:
        print(report)
