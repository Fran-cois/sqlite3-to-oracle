"""
Module pour valider l'importation du schéma et des données dans Oracle.
"""

import oracledb
import sqlite3
import re
import datetime
from typing import Dict, List, Tuple, Any, Optional
from . import logger
from .rich_logging import print_title, print_success_message, print_error_message, print_warning_message, RICH_AVAILABLE
from .converter import sanitize_sql_value

def connect_to_oracle(config: Dict[str, str]) -> Tuple[Optional[oracledb.Connection], Optional[str]]:
    """
    Établit une connexion à la base de données Oracle.
    
    Args:
        config: Configuration Oracle (user, password, dsn)
        
    Returns:
        Tuple contenant (connexion, message d'erreur)
    """
    try:
        conn = oracledb.connect(
            user=config["user"],
            password=config["password"],
            dsn=config["dsn"]
        )
        return conn, None
    except Exception as e:
        return None, str(e)

def connect_to_sqlite(sqlite_path: str) -> Tuple[Optional[sqlite3.Connection], Optional[str]]:
    """
    Établit une connexion à la base de données SQLite.
    
    Args:
        sqlite_path: Chemin vers le fichier SQLite
        
    Returns:
        Tuple contenant (connexion, message d'erreur)
    """
    try:
        conn = sqlite3.connect(sqlite_path)
        return conn, None
    except Exception as e:
        return None, str(e)

def get_sqlite_tables(conn: sqlite3.Connection) -> List[str]:
    """
    Récupère la liste des tables dans la base SQLite.
    
    Args:
        conn: Connexion SQLite
        
    Returns:
        Liste des noms de tables
    """
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables

def get_oracle_tables(conn: oracledb.Connection) -> List[str]:
    """
    Récupère la liste des tables dans la base Oracle.
    
    Args:
        conn: Connexion Oracle
        
    Returns:
        Liste des noms de tables
    """
    cursor = conn.cursor()
    cursor.execute("SELECT table_name FROM user_tables")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables

def get_sqlite_table_schema(conn: sqlite3.Connection, table: str) -> List[Tuple[str, str]]:
    """
    Récupère le schéma d'une table SQLite.
    
    Args:
        conn: Connexion SQLite
        table: Nom de la table
        
    Returns:
        Liste de tuples (nom_colonne, type_colonne)
    """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table})")
    columns = [(row[1], row[2]) for row in cursor.fetchall()]
    cursor.close()
    return columns

def get_oracle_table_schema(conn: oracledb.Connection, table: str) -> List[Tuple[str, str]]:
    """
    Récupère le schéma d'une table Oracle.
    
    Args:
        conn: Connexion Oracle
        table: Nom de la table
        
    Returns:
        Liste de tuples (nom_colonne, type_colonne)
    """
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT column_name, data_type
        FROM user_tab_columns
        WHERE table_name = '{table.upper()}'
    """)
    columns = [(row[0], row[1]) for row in cursor.fetchall()]
    cursor.close()
    return columns

def get_sqlite_row_count(conn: sqlite3.Connection, table: str) -> int:
    """
    Récupère le nombre de lignes dans une table SQLite.
    
    Args:
        conn: Connexion SQLite
        table: Nom de la table
        
    Returns:
        Nombre de lignes
    """
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    cursor.close()
    return count

def get_oracle_row_count(conn: oracledb.Connection, table: str) -> int:
    """
    Récupère le nombre de lignes dans une table Oracle.
    
    Args:
        conn: Connexion Oracle
        table: Nom de la table
        
    Returns:
        Nombre de lignes
    """
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    cursor.close()
    return count

def map_sqlite_type_to_oracle(sqlite_type: str) -> str:
    """
    Mappe un type SQLite vers son équivalent Oracle.
    
    Args:
        sqlite_type: Type SQLite
        
    Returns:
        Type Oracle équivalent
    """
    type_map = {
        "INTEGER": "NUMBER",
        "INT": "NUMBER",
        "REAL": "FLOAT",
        "TEXT": "VARCHAR2",
        "VARCHAR": "VARCHAR2",
        "DATETIME": "DATE",
        "DATE": "DATE",
        "BLOB": "BLOB",
        "BOOLEAN": "NUMBER(1)",
    }
    
    # Gérer les types avec taille (VARCHAR(255) -> VARCHAR2)
    base_type = re.sub(r'\(.*\)', '', sqlite_type.upper())
    
    # Retourner le type Oracle équivalent ou le type SQLite si aucune correspondance
    return type_map.get(base_type, sqlite_type)

def compare_types(sqlite_type: str, oracle_type: str) -> bool:
    """
    Compare un type SQLite et un type Oracle pour vérifier leur équivalence.
    
    Args:
        sqlite_type: Type SQLite
        oracle_type: Type Oracle
        
    Returns:
        True si les types sont équivalents, False sinon
    """
    # Convertir le type SQLite vers son équivalent Oracle
    expected_oracle_type = map_sqlite_type_to_oracle(sqlite_type)
    
    # Comparer les bases des types (ignorer les tailles)
    base_expected = re.sub(r'\(.*\)', '', expected_oracle_type.upper())
    base_actual = re.sub(r'\(.*\)', '', oracle_type.upper())
    
    # Gérer les cas spéciaux
    if base_expected == "NUMBER" and base_actual in ["INTEGER", "NUMBER", "FLOAT"]:
        return True
    if base_expected == "VARCHAR2" and base_actual in ["VARCHAR2", "CLOB", "NVARCHAR2", "CHAR"]:
        return True
    
    return base_expected == base_actual

def validate_schema(
    sqlite_conn: sqlite3.Connection, 
    oracle_conn: oracledb.Connection, 
    verbose: bool = False
) -> Dict[str, Any]:
    """
    Valide que le schéma SQLite a été correctement importé dans Oracle.
    
    Args:
        sqlite_conn: Connexion SQLite
        oracle_conn: Connexion Oracle
        verbose: Afficher des informations détaillées
        
    Returns:
        Dictionnaire de résultats de validation
    """
    # Récupérer les tables
    sqlite_tables = get_sqlite_tables(sqlite_conn)
    oracle_tables = get_oracle_tables(oracle_conn)
    
    # Normaliser les noms de tables Oracle (les convertir en majuscules)
    oracle_tables = [table.upper() for table in oracle_tables]
    
    # Détecter les tables qui semblent avoir été créées avec une structure simplifiée
    simplified_tables = []
    try:
        # Vérifier les tables qui ont exactement les colonnes standard des tables simplifiées
        cursor = oracle_conn.cursor()
        for table in oracle_tables:
            try:
                # Vérifier si la table a les colonnes standard de la structure simplifiée
                query = f"""
                SELECT COUNT(*) FROM user_tab_columns 
                WHERE table_name = '{table}' 
                AND column_name IN ('ID', 'NAME', 'VALUE', 'CREATED_DATE', 'DESCRIPTION')
                """
                cursor.execute(query)
                standard_cols_count = cursor.fetchone()[0]
                
                query = f"SELECT COUNT(*) FROM user_tab_columns WHERE table_name = '{table}'"
                cursor.execute(query)
                total_cols_count = cursor.fetchone()[0]
                
                # Si toutes les colonnes sont des colonnes standard et il y en a peu (2-5)
                if standard_cols_count >= 2 and standard_cols_count == total_cols_count and total_cols_count <= 5:
                    simplified_tables.append(table)
                    logger.debug(f"Table {table} détectée comme table simplifiée")
            except:
                pass
        cursor.close()
    except:
        logger.debug("Impossible de détecter les tables simplifiées")
    
    # Résultats de validation
    results = {
        "success": True,
        "tables": {
            "total_sqlite": len(sqlite_tables),
            "total_oracle": len(oracle_tables),
            "missing": [],
            "simplified": simplified_tables,
            "details": {}
        },
        "data": {
            "tables_with_issues": 0,
            "total_row_count_sqlite": 0,
            "total_row_count_oracle": 0,
            "tables_with_missing_data": []
        }
    }
    
    # Vérifier les tables manquantes
    for table in sqlite_tables:
        oracle_table = table.upper()
        if oracle_table not in oracle_tables:
            results["success"] = False
            results["tables"]["missing"].append(table)
    
    if results["tables"]["missing"]:
        missing_msg = f"{len(results['tables']['missing'])} tables manquantes dans Oracle"
        print_error_message(missing_msg)
        logger.error(missing_msg)
        
        if verbose:
            for table in results["tables"]["missing"]:
                logger.error(f"Table manquante: {table}")
    else:
        success_msg = "Toutes les tables SQLite ont été créées dans Oracle"
        print_success_message(success_msg)
        logger.info(success_msg)
    
    # Vérifier les colonnes et comparer les schémas
    for table in sqlite_tables:
        oracle_table = table.upper()
        
        # Ignorer les tables qui n'ont pas été créées
        if oracle_table not in oracle_tables:
            continue
        
        sqlite_schema = get_sqlite_table_schema(sqlite_conn, table)
        oracle_schema = get_oracle_table_schema(oracle_conn, oracle_table)
        
        # Compter les lignes
        sqlite_count = get_sqlite_row_count(sqlite_conn, table)
        oracle_count = get_oracle_row_count(oracle_conn, oracle_table)
        
        results["data"]["total_row_count_sqlite"] += sqlite_count
        results["data"]["total_row_count_oracle"] += oracle_count
        
        # Vérifier si des données sont manquantes
        if sqlite_count > oracle_count:
            results["data"]["tables_with_missing_data"].append({
                "table": table,
                "sqlite_count": sqlite_count,
                "oracle_count": oracle_count,
                "missing": sqlite_count - oracle_count
            })
        
        # Comparaison des colonnes
        sqlite_columns = {col[0].upper(): col[1] for col in sqlite_schema}
        oracle_columns = {col[0].upper(): col[1] for col in oracle_schema}
        
        # Identifier les colonnes manquantes et les différences de type
        missing_columns = []
        type_mismatches = []
        
        for col_name, col_type in sqlite_columns.items():
            if col_name not in oracle_columns:
                missing_columns.append(col_name)
            elif not compare_types(col_type, oracle_columns[col_name]):
                type_mismatches.append({
                    "column": col_name,
                    "sqlite_type": col_type,
                    "oracle_type": oracle_columns[col_name]
                })
        
        # Stocker les résultats
        results["tables"]["details"][table] = {
            "sqlite_columns": len(sqlite_columns),
            "oracle_columns": len(oracle_columns),
            "missing_columns": missing_columns,
            "type_mismatches": type_mismatches,
            "sqlite_rows": sqlite_count,
            "oracle_rows": oracle_count
        }
        
        # Mettre à jour le statut global
        if missing_columns or type_mismatches:
            results["success"] = False
            results["data"]["tables_with_issues"] += 1
    
    # Afficher les résultats des colonnes et types
    if results["data"]["tables_with_issues"] > 0:
        schema_issues_msg = f"{results['data']['tables_with_issues']} tables présentent des différences de schéma"
        print_warning_message(schema_issues_msg)
        logger.warning(schema_issues_msg)
        
        if verbose:
            for table, details in results["tables"]["details"].items():
                if details["missing_columns"] or details["type_mismatches"]:
                    table_msg = f"Table {table}"
                    logger.warning(table_msg)
                    
                    if details["missing_columns"]:
                        missing_cols_msg = f"  - Colonnes manquantes: {', '.join(details['missing_columns'])}"
                        logger.warning(missing_cols_msg)
                    
                    if details["type_mismatches"]:
                        for mismatch in details["type_mismatches"]:
                            type_msg = f"  - Type différent pour {mismatch['column']}: SQLite={mismatch['sqlite_type']}, Oracle={mismatch['oracle_type']}"
                            logger.warning(type_msg)
    else:
        schema_success_msg = "Tous les schémas de tables correspondent"
        print_success_message(schema_success_msg)
        logger.info(schema_success_msg)
    
    # Afficher les résultats des données
    if results["data"]["total_row_count_sqlite"] > results["data"]["total_row_count_oracle"]:
        missing_rows = results["data"]["total_row_count_sqlite"] - results["data"]["total_row_count_oracle"]
        missing_data_msg = f"{missing_rows} lignes manquantes dans Oracle"
        print_warning_message(missing_data_msg)
        logger.warning(missing_data_msg)
        
        if verbose and results["data"]["tables_with_missing_data"]:
            for table_info in results["data"]["tables_with_missing_data"]:
                missing_table_msg = f"Table {table_info['table']}: {table_info['missing']} lignes manquantes"
                logger.warning(missing_table_msg)
    else:
        data_success_msg = "Toutes les données ont été importées"
        print_success_message(data_success_msg)
        logger.info(data_success_msg)
    
    # Afficher un récapitulatif des statistiques
    print("\n" + "=" * 50)
    print("RÉCAPITULATIF DE LA VALIDATION")
    print("=" * 50)
    print(f"Tables SQLite: {results['tables']['total_sqlite']}")
    print(f"Tables Oracle: {results['tables']['total_oracle']}")
    print(f"Tables avec structure simplifiée: {len(simplified_tables)}")
    print(f"Tables avec problèmes de schéma: {results['data']['tables_with_issues']}")
    print(f"Lignes dans SQLite: {results['data']['total_row_count_sqlite']}")
    print(f"Lignes dans Oracle: {results['data']['total_row_count_oracle']}")
    
    # Calculer le pourcentage de données importées avec succès
    if results['data']['total_row_count_sqlite'] > 0:
        success_percentage = (results['data']['total_row_count_oracle'] / results['data']['total_row_count_sqlite']) * 100
        print(f"Pourcentage de données importées: {success_percentage:.2f}%")
    
    print(f"Tables avec données manquantes: {len(results['data']['tables_with_missing_data'])}")
    
    # Ajouter des détails supplémentaires en mode verbose
    if verbose:
        # Détails des tables avec structure simplifiée
        if simplified_tables:
            print("\n" + "-" * 50)
            print("TABLES AVEC STRUCTURE SIMPLIFIÉE")
            print("-" * 50)
            for table in simplified_tables:
                print(f"- {table}")
            print("\nCes tables ont été créées avec une structure simplifiée car leur schéma d'origine n'était pas compatible avec Oracle.")
            print("Les données d'origine n'ont pas pu être importées pour ces tables.")
        
        # Détails des tables avec problèmes de schéma
        if results['data']['tables_with_issues'] > 0:
            print("\n" + "-" * 50)
            print("DÉTAILS DES PROBLÈMES DE SCHÉMA")
            print("-" * 50)
            for table, details in results['tables']['details'].items():
                if details['missing_columns'] or details['type_mismatches']:
                    print(f"\nTable: {table}")
                    if details['missing_columns']:
                        print(f"  Colonnes manquantes ({len(details['missing_columns'])}):")
                        for col in details['missing_columns']:
                            print(f"    - {col}")
                    if details['type_mismatches']:
                        print(f"  Types incompatibles ({len(details['type_mismatches'])}):")
                        for mismatch in details['type_mismatches']:
                            print(f"    - {mismatch['column']}: SQLite={mismatch['sqlite_type']}, Oracle={mismatch['oracle_type']}")
        
        # Détails des tables avec données manquantes
        if results['data']['tables_with_missing_data']:
            print("\n" + "-" * 50)
            print("DÉTAILS DES DONNÉES MANQUANTES")
            print("-" * 50)
            for table_info in results['data']['tables_with_missing_data']:
                percentage = (table_info['oracle_count'] / table_info['sqlite_count']) * 100 if table_info['sqlite_count'] > 0 else 0
                print(f"\nTable: {table_info['table']}")
                print(f"  Lignes dans SQLite: {table_info['sqlite_count']}")
                print(f"  Lignes dans Oracle: {table_info['oracle_count']}")
                print(f"  Lignes manquantes: {table_info['missing']} ({100 - percentage:.2f}% des données)")
        
        # Statistiques globales des colonnes
        total_columns_sqlite = sum(details['sqlite_columns'] for details in results['tables']['details'].values())
        total_columns_oracle = sum(details['oracle_columns'] for details in results['tables']['details'].values())
        print("\n" + "-" * 50)
        print("STATISTIQUES GLOBALES")
        print("-" * 50)
        print(f"Total des colonnes SQLite: {total_columns_sqlite}")
        print(f"Total des colonnes Oracle: {total_columns_oracle}")
        
        # Ajouter un résumé des tables importées avec succès
        successful_tables = [table for table, details in results['tables']['details'].items() 
                           if not details['missing_columns'] and not details['type_mismatches'] and 
                           details['sqlite_rows'] == details['oracle_rows']]
        
        print(f"\nTables importées avec succès (schéma et données): {len(successful_tables)}/{len(results['tables']['details'])}")
        if successful_tables:
            print("Tables validées sans problème:")
            for i, table in enumerate(sorted(successful_tables)):
                print(f"  {i+1}. {table}")
    
    print("=" * 50)
    
    if results["success"]:
        print_success_message("VALIDATION RÉUSSIE")
    else:
        print_warning_message("VALIDATION AVEC AVERTISSEMENTS")
    
    return results

def insert_data_from_sqlite(oracle_cursor, sqlite_cursor, table_name, columns, batch_size=100):
    """
    Insère les données depuis SQLite vers Oracle pour une table donnée.
    
    Args:
        oracle_cursor: Curseur Oracle
        sqlite_cursor: Curseur SQLite
        table_name: Nom de la table
        columns: Liste des colonnes
        batch_size: Nombre de lignes à insérer par batch
    """
    # Récupérer les données de SQLite
    sqlite_cursor.execute(f"SELECT * FROM {table_name}")
    rows = sqlite_cursor.fetchall()
    
    if not rows:
        logger.debug(f"Aucune donnée à insérer pour la table {table_name}")
        return 0
    
    # Préparer la requête d'insertion avec bind variables
    placeholders = ", ".join([f":{i+1}" for i in range(len(columns))])
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    
    inserted = 0
    errors = 0
    
    try:
        # Insérer par batch
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            batch_data = []
            
            # Sanitiser les données de chaque ligne
            for row in batch:
                sanitized_row = [sanitize_sql_value(value) if isinstance(value, str) else value for value in row]
                batch_data.append(sanitized_row)
            
            try:
                oracle_cursor.executemany(insert_query, batch_data)
                inserted += len(batch)
            except Exception as e:
                # Essayer d'insérer ligne par ligne en cas d'erreur
                for row in batch_data:
                    try:
                        oracle_cursor.execute(insert_query, row)
                        inserted += 1
                    except Exception as row_error:
                        errors += 1
                        logger.debug(f"Erreur lors de l'insertion dans {table_name}: {str(row_error)}")
                        # Afficher les 50 premiers caractères de la ligne problématique
                        logger.debug(f"Données: {str(row)[:50]}...")
        
        logger.info(f"Insertion des données dans {table_name} ({inserted} lignes, {errors} erreurs)...")
        return inserted
    except Exception as e:
        error_code = None
        if hasattr(e, 'args') and e.args:
            error_info = e.args[0]
            if hasattr(error_info, 'code'):
                error_code = error_info.code
        
        logger.debug(f"Erreur {error_code or 'inconnue'} lors de l'insertion dans {table_name}: {str(e)}")
        if "ORA-00917" in str(e):
            logger.debug("Help: https://docs.oracle.com/error-help/db/ora-00917/")
        return 0

def run_validation(
    sqlite_path: str,
    oracle_config: Dict[str, str],
    verbose: bool = False
) -> bool:
    """
    Exécute la validation complète du schéma et des données.
    
    Args:
        sqlite_path: Chemin vers le fichier SQLite
        oracle_config: Configuration Oracle
        verbose: Afficher des informations détaillées
        
    Returns:
        True si la validation réussit, False sinon
    """
    print_title("Validation du schéma et des données")
    
    # Connexion à SQLite
    logger.info(f"Connexion à la base SQLite: {sqlite_path}")
    sqlite_conn, sqlite_error = connect_to_sqlite(sqlite_path)
    if sqlite_error:
        print_error_message(f"Erreur de connexion à SQLite: {sqlite_error}")
        return False
    
    # Connexion à Oracle
    logger.info(f"Connexion à Oracle: {oracle_config['user']}@{oracle_config['dsn']}")
    oracle_conn, oracle_error = connect_to_oracle(oracle_config)
    if oracle_error:
        print_error_message(f"Erreur de connexion à Oracle: {oracle_error}")
        sqlite_conn.close()
        return False
    
    try:
        # Exécuter la validation
        logger.info("Comparaison des schémas et des données...")
        results = validate_schema(sqlite_conn, oracle_conn, verbose)
        
        # Afficher le résumé
        if results["success"]:
            success_msg = "Validation réussie: Le schéma et les données ont été correctement importés"
            print_success_message(success_msg)
            logger.info(success_msg)
        else:
            warning_msg = "Validation terminée avec des problèmes"
            print_warning_message(warning_msg)
            logger.warning(warning_msg)
            
        return results["success"]
    
    finally:
        # Fermeture des connexions
        sqlite_conn.close()
        oracle_conn.close()
