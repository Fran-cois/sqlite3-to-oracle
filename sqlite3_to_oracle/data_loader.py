"""
Module pour charger des données dans Oracle avec différentes stratégies.
Ce module offre des alternatives pour les cas où l'approche standard échoue.
"""

import sqlite3
import oracledb
import re
import csv
import os
import tempfile
import time
from typing import Dict, List, Tuple, Optional, Iterator, Any
from . import logger
from .lookup_loader import create_simplified_lookup_table, parse_and_load_lookup_data
from .performance_loader import load_performance_table

def extract_table_structure(sqlite_path: str, table_name: str) -> Tuple[List[str], List[str]]:
    """
    Extrait la structure d'une table SQLite.
    
    Args:
        sqlite_path: Chemin vers le fichier SQLite
        table_name: Nom de la table à analyser
        
    Returns:
        Tuple contenant (colonnes, types)
    """
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    
    # Récupérer les infos sur les colonnes
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = []
    types = []
    
    for row in cursor.fetchall():
        columns.append(row[1])  # Nom de colonne
        types.append(row[2])    # Type de colonne
    
    cursor.close()
    conn.close()
    
    return columns, types

def export_table_to_csv(sqlite_path: str, table_name: str, chunk_size: int = 50000) -> str:
    """
    Exporte une table SQLite vers un fichier CSV temporaire avec gestion des gros volumes de données.
    
    Args:
        sqlite_path: Chemin vers le fichier SQLite
        table_name: Nom de la table à exporter
        chunk_size: Nombre de lignes à traiter par lot pour économiser la mémoire
        
    Returns:
        Chemin vers le fichier CSV créé
    """
    # Créer un fichier temporaire pour le CSV
    temp_dir = tempfile.gettempdir()
    csv_path = os.path.join(temp_dir, f"{table_name}_{int(time.time())}.csv")
    
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    
    # Récupérer les noms de colonnes
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    
    # Obtenir le nombre total de lignes pour le reporting
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    total_rows = cursor.fetchone()[0]
    
    # Écrire dans le CSV par lots pour économiser la mémoire
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(columns)  # En-tête
        
        # Traitement par lots
        offset = 0
        rows_written = 0
        
        while offset < total_rows:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset}")
            batch = cursor.fetchall()
            
            if not batch:
                break
                
            writer.writerows(batch)
            
            rows_written += len(batch)
            offset += chunk_size
            
            # Afficher la progression tous les 100 000 enregistrements
            if rows_written % 100000 == 0 or rows_written == total_rows:
                progress_pct = (rows_written / total_rows) * 100 if total_rows > 0 else 100
                logger.info(f"Export de {table_name}: {rows_written}/{total_rows} lignes ({progress_pct:.1f}%)")
    
    cursor.close()
    conn.close()
    
    logger.info(f"Données de {table_name} exportées vers {csv_path} ({rows_written} lignes)")
    return csv_path

def create_table_from_sqlite(
    oracle_conn: oracledb.Connection, 
    sqlite_path: str, 
    table_name: str,
    analyze_constraints: bool = True
) -> bool:
    """
    Crée une table Oracle basée sur le schéma d'une table SQLite.
    Approche conservatrice qui fonctionne pour les tables volumineuses.
    
    Args:
        oracle_conn: Connexion Oracle
        sqlite_path: Chemin vers le fichier SQLite
        table_name: Nom de la table à créer
        analyze_constraints: Si True, analyse et ajoute les contraintes (clés étrangères, etc.)
        
    Returns:
        True si la création a réussi, False sinon
    """
    cursor = oracle_conn.cursor()
    
    try:
        # Récupérer la structure de la table SQLite
        columns, types = extract_table_structure(sqlite_path, table_name)
        
        # Mapper les types SQLite vers Oracle
        oracle_columns = []
        primary_key_cols = []
        
        for i, (col, type_) in enumerate(zip(columns, types)):
            # Mapper le type
            if type_.upper() in ('INTEGER', 'INT'):
                oracle_type = 'NUMBER'
            elif type_.upper() == 'TEXT':
                oracle_type = 'VARCHAR2(4000)'
            elif type_.upper() == 'REAL':
                oracle_type = 'NUMBER(38,10)'
            elif type_.upper().startswith('CHAR'):
                size_match = re.search(r'CHAR\((\d+)\)', type_, re.IGNORECASE)
                if size_match:
                    size = min(int(size_match.group(1)), 4000)
                    oracle_type = f"VARCHAR2({size})"
                else:
                    oracle_type = 'VARCHAR2(1)'
            elif type_.upper().startswith('VARCHAR'):
                size_match = re.search(r'VARCHAR\((\d+)\)', type_, re.IGNORECASE)
                if size_match:
                    size = min(int(size_match.group(1)), 4000)
                    oracle_type = f"VARCHAR2({size})"
                else:
                    oracle_type = 'VARCHAR2(255)'
            elif type_.upper() == 'BLOB':
                oracle_type = 'BLOB'
            elif type_.upper() == 'BOOLEAN':
                oracle_type = 'NUMBER(1)'
            else:
                oracle_type = 'VARCHAR2(255)'  # Type par défaut
            
            # Construire la définition de colonne
            col_def = f"{col} {oracle_type}"
            
            # Identifier une colonne potentielle pour la clé primaire
            if col.upper() in ('ID', 'CODE', f"{table_name.upper()}_ID") or col.upper().endswith('_ID'):
                col_def += " NOT NULL"
                primary_key_cols.append(col)
            
            oracle_columns.append(col_def)
        
        # Créer la contrainte de clé primaire
        pk_constraint = ""
        if primary_key_cols:
            # Prendre uniquement la première colonne comme clé primaire pour simplifier
            pk_col = primary_key_cols[0]
            pk_constraint = ",\n  CONSTRAINT PK_" + table_name + " PRIMARY KEY (" + pk_col + ")"
        
        # Ajouter les contraintes de clé étrangère si demandé
        fk_constraints = []
        if analyze_constraints:
            conn = sqlite3.connect(sqlite_path)
            try:
                fk_cursor = conn.cursor()
                fk_cursor.execute(f"PRAGMA foreign_key_list({table_name})")
                for fk in fk_cursor.fetchall():
                    ref_table = fk[2]  # table référencée
                    from_col = fk[3]   # colonne source
                    to_col = fk[4]     # colonne référencée
                    
                    # Éviter les noms de contrainte trop longs (max 30 caractères dans Oracle)
                    constraint_name = f"FK_{table_name[:10]}_{from_col[:10]}"
                    fk_stmt = f",\n  CONSTRAINT {constraint_name} FOREIGN KEY ({from_col}) REFERENCES {ref_table}({to_col})"
                    fk_constraints.append(fk_stmt)
                fk_cursor.close()
            except Exception as e:
                logger.debug(f"Impossible d'analyser les clés étrangères: {str(e)}")
            finally:
                conn.close()
        
        # Construire et exécuter la requête CREATE TABLE
        create_stmt = "CREATE TABLE " + table_name + " (\n  "
        create_stmt += ",\n  ".join(oracle_columns)
        create_stmt += pk_constraint
        
        # Ajouter les contraintes de clé étrangère
        for fk in fk_constraints:
            create_stmt += fk
            
        create_stmt += "\n)"
        
        # Log de la requête en mode debug
        logger.debug(f"Requête CREATE TABLE: {create_stmt}")
        
        cursor.execute(create_stmt)
        oracle_conn.commit()
        
        logger.info(f"Table {table_name} créée avec succès ({len(columns)} colonnes)")
        return True
    
    except Exception as e:
        logger.error(f"Erreur lors de la création de la table {table_name}: {str(e)}")
        
        # Collecter des informations de diagnostic supplémentaires sur l'erreur
        error_info = str(e)
        
        # Vérifier si l'erreur est liée à des problèmes courants
        if "ORA-00972" in error_info:  # Identifier too long
            logger.warning("Problème de nom d'identifiant trop long. Tentative de création simplifiée...")
        elif "ORA-00907" in error_info:  # Missing right parenthesis
            logger.warning("Problème de parenthèse manquante. Tentative de création simplifiée...")
        elif "ORA-00904" in error_info:  # Invalid identifier
            logger.warning("Identifiant invalide. Tentative de création simplifiée...")
        
        try:
            # En cas d'échec, créer une structure minimale
            minimal_stmt = """
            CREATE TABLE """ + table_name + """ (
              ID NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
              COLUMN_NAME VARCHAR2(255),
              VALUE VARCHAR2(4000),
              ROW_NUM NUMBER,
              CREATED_DATE DATE DEFAULT SYSDATE
            )
            """
            cursor.execute(minimal_stmt)
            oracle_conn.commit()
            logger.warning(f"Table {table_name} créée avec structure minimale de secours")
            return False
        except Exception as e2:
            logger.error(f"Échec de la création minimale pour {table_name}: {str(e2)}")
            return False
    finally:
        cursor.close()

def load_csv_to_oracle(
    oracle_conn: oracledb.Connection,
    csv_path: str,
    table_name: str,
    has_original_structure: bool,
    batch_size: int = 5000,  # Optimisé pour de meilleures performances
    max_retries: int = 3     # Ajout d'un mécanisme de réessai
) -> int:
    """
    Charge les données d'un fichier CSV dans une table Oracle avec optimisations.
    
    Args:
        oracle_conn: Connexion Oracle
        csv_path: Chemin vers le fichier CSV
        table_name: Nom de la table cible
        has_original_structure: Si True, la table a sa structure originale
                               Si False, utiliser la structure minimale
        batch_size: Nombre de lignes à insérer par lot
        max_retries: Nombre maximum de tentatives en cas d'erreur
                               
    Returns:
        Nombre de lignes chargées
    """
    cursor = oracle_conn.cursor()
    loaded_rows = 0
    
    try:
        # Lire le CSV
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            # Compter le nombre total de lignes pour le reporting
            total_lines = sum(1 for _ in csvfile)
            csvfile.seek(0)  # Revenir au début du fichier
            
            reader = csv.reader(csvfile)
            headers = next(reader)  # Première ligne = en-têtes
            
            if has_original_structure:
                # Pour les tables avec structure originale, utiliser des INSERTs directs
                placeholder = ", ".join([f":{i+1}" for i in range(len(headers))])
                insert_sql = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({placeholder})"
                
                # Vérifier si la table a un support de transactions (pour le rollback)
                has_tx_support = True
                try:
                    cursor.execute(f"SELECT table_name FROM user_tables WHERE table_name = '{table_name.upper()}'")
                    if cursor.fetchone():
                        cursor.execute(f"SELECT tablespace_name FROM user_tables WHERE table_name = '{table_name.upper()}'")
                        tablespace = cursor.fetchone()[0]
                        cursor.execute(f"SELECT tablespace_name, contents FROM dba_tablespaces WHERE tablespace_name = '{tablespace}'")
                        result = cursor.fetchone()
                        if result and result[1] == 'TEMPORARY':
                            has_tx_support = False
                except:
                    # En cas de doute, supposer que la table supporte les transactions
                    pass
                
                # Utiliser executemany pour une meilleure performance
                batch = []
                lines_read = 1  # Commence à 1 car l'en-tête est déjà lu
                
                for row in reader:
                    lines_read += 1
                    
                    # Sanitiser les valeurs du CSV
                    sanitized_row = []
                    for val in row:
                        if val == '':  # Traiter les valeurs vides comme NULL
                            sanitized_row.append(None)
                        else:
                            sanitized_row.append(val)
                    
                    batch.append(sanitized_row)
                    
                    if len(batch) >= batch_size:
                        # Afficher la progression
                        if lines_read % (batch_size * 10) == 0:
                            progress_pct = (lines_read / total_lines) * 100
                            logger.info(f"Chargement de {table_name}: {lines_read}/{total_lines} lignes ({progress_pct:.1f}%)")
                        
                        # Exécuter le batch avec mécanisme de réessai
                        success, rows_inserted = _execute_batch_with_retry(
                            oracle_conn, cursor, insert_sql, batch, max_retries, has_tx_support
                        )
                        
                        loaded_rows += rows_inserted
                        batch = []
                
                # Traiter le dernier lot
                if batch:
                    success, rows_inserted = _execute_batch_with_retry(
                        oracle_conn, cursor, insert_sql, batch, max_retries, has_tx_support
                    )
                    loaded_rows += rows_inserted
            else:
                # Pour les tables avec structure minimale, stocker en format pivot
                row_num = 0
                for row in reader:
                    row_num += 1
                    
                    # Insérer chaque valeur comme une ligne séparée
                    for i, (col_name, value) in enumerate(zip(headers, row)):
                        if value:  # Ne pas insérer les valeurs vides
                            try:
                                cursor.execute(
                                    f"INSERT INTO {table_name} (COLUMN_NAME, VALUE, ROW_NUM) VALUES (:1, :2, :3)",
                                    [col_name, str(value)[:4000], row_num]
                                )
                                oracle_conn.commit()
                                loaded_rows += 1
                            except Exception as e:
                                logger.debug(f"Échec d'insertion dans {table_name}: {str(e)}")
        
        logger.info(f"Chargement terminé pour {table_name}: {loaded_rows} lignes insérées")
        return loaded_rows
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement des données pour {table_name}: {str(e)}")
        return loaded_rows
    finally:
        cursor.close()

def _execute_batch_with_retry(
    conn: oracledb.Connection, 
    cursor: oracledb.Cursor, 
    sql: str, 
    batch: List[List[Any]], 
    max_retries: int,
    has_tx_support: bool
) -> Tuple[bool, int]:
    """
    Exécute un lot d'insertions avec mécanisme de réessai pour améliorer la fiabilité.
    
    Args:
        conn: Connexion Oracle
        cursor: Curseur Oracle
        sql: Instruction SQL à exécuter
        batch: Lot de données à insérer
        max_retries: Nombre maximum de tentatives
        has_tx_support: Si True, utiliser le support des transactions
        
    Returns:
        Tuple (succès, nombre de lignes insérées)
    """
    rows_inserted = 0
    retries = 0
    
    while retries < max_retries:
        try:
            cursor.executemany(sql, batch)
            conn.commit()
            return True, len(batch)
        except Exception as e:
            retries += 1
            error_info = str(e)
            
            # Si c'est la dernière tentative, essayer ligne par ligne
            if retries >= max_retries:
                logger.warning(f"Échec du lot après {retries} tentatives, essai ligne par ligne: {error_info}")
                
                # Essayer chaque ligne individuellement
                for i, row in enumerate(batch):
                    try:
                        cursor.execute(sql, row)
                        if has_tx_support:
                            conn.commit()
                        rows_inserted += 1
                    except Exception as row_error:
                        # Si une ligne échoue, continuer avec les autres
                        logger.debug(f"Ligne {i} ignorée: {str(row_error)}")
                
                # Commit final si pas de support de transactions par ligne
                if not has_tx_support:
                    try:
                        conn.commit()
                    except:
                        pass
                        
                return False, rows_inserted
            else:
                # Log et réessayer
                logger.debug(f"Échec du lot (tentative {retries}/{max_retries}): {error_info}")
                time.sleep(0.5)  # Pause avant de réessayer
                if has_tx_support:
                    try:
                        conn.rollback()
                    except:
                        pass

def load_table_alternative(
    oracle_config: Dict[str, str],
    sqlite_path: str,
    table_name: str,
    sql_file_path: str = None,
    batch_size: int = 5000,
    use_varchar_for_decimals: bool = False
) -> bool:
    """
    Charge une table SQLite dans Oracle en utilisant une approche alternative
    via CSV quand la méthode standard échoue.
    
    Args:
        oracle_config: Configuration Oracle
        sqlite_path: Chemin vers le fichier SQLite
        table_name: Nom de la table à charger
        sql_file_path: Chemin vers le fichier SQL original (optionnel)
        batch_size: Taille des lots pour l'insertion
        use_varchar_for_decimals: Si True, utilise VARCHAR2 pour les nombres décimaux
        
    Returns:
        True si le chargement a réussi, False sinon
    """
    logger.info(f"Chargement alternatif pour la table {table_name}")
    
    # Détection des grandes tables ou tables spéciales qui nécessitent un traitement particulier
    if "ON_TIME_PERFORMANCE" in table_name.upper() or "ON_TIME_ON_TIME" in table_name.upper():
        return load_performance_table(oracle_config, sqlite_path, table_name, use_varchar_for_decimals)
    
    # Détection des tables de lookup (L_*)
    if table_name.startswith("L_") and sql_file_path:
        try:
            conn = oracledb.connect(
                user=oracle_config["user"],
                password=oracle_config["password"],
                dsn=oracle_config["dsn"]
            )
            success = parse_and_load_lookup_data(conn, sql_file_path, table_name)
            conn.close()
            return success
        except Exception as e:
            logger.warning(f"Échec du chargement par parse_and_load_lookup_data: {str(e)}")
            # Continuer avec la méthode standard ci-dessous
    
    try:
        # Étape 1: Connexion à Oracle
        conn = oracledb.connect(
            user=oracle_config["user"],
            password=oracle_config["password"],
            dsn=oracle_config["dsn"]
        )
        
        # Étape 2: Créer la table dans Oracle
        success = create_table_from_sqlite(conn, sqlite_path, table_name)
        
        # Étape 3: Exporter les données vers CSV
        csv_path = export_table_to_csv(sqlite_path, table_name)
        
        # Mesurer le temps de chargement
        start_time = time.time()
        
        # Étape 4: Charger les données dans Oracle
        rows_loaded = load_csv_to_oracle(conn, csv_path, table_name, success, batch_size)
        
        # Étape 5: Nettoyage et statistiques
        elapsed_time = time.time() - start_time
        rows_per_second = int(rows_loaded / elapsed_time) if elapsed_time > 0 else 0
        
        try:
            os.remove(csv_path)
            logger.debug(f"Fichier temporaire supprimé: {csv_path}")
        except:
            pass
        
        # Fermer la connexion
        conn.close()
        
        if rows_loaded > 0:
            logger.info(f"Performance: {rows_loaded} lignes chargées en {elapsed_time:.2f} secondes ({rows_per_second} lignes/sec)")
            return True
        return False
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement alternatif pour {table_name}: {str(e)}")
        return False

def load_failing_tables(
    oracle_config: Dict[str, str],
    sql_file_path: str,
    failed_tables: List[str]
) -> Dict[str, bool]:
    """
    Tente de charger les tables qui ont échoué lors de l'importation standard.
    
    Args:
        oracle_config: Configuration Oracle
        sql_file_path: Chemin vers le fichier SQL original
        failed_tables: Liste des noms de tables qui ont échoué
        
    Returns:
        Dictionnaire des résultats par table
    """
    results = {}
    
    try:
        logger.info(f"Tentative de chargement alternatif pour {len(failed_tables)} tables")
        
        for table_name in failed_tables:
            # Déterminer si c'est une table volumineuse
            is_large_table = "ON_TIME" in table_name.upper()
            
            # La fonction détecte automatiquement s'il s'agit d'une table de lookup
            is_lookup = table_name.startswith('L_')
            
            if is_large_table:
                # Récupérer le chemin du fichier SQLite à partir du SQL
                sqlite_path = None
                if "sqlite3" in sql_file_path:
                    sqlite_base = os.path.basename(sql_file_path)
                    sqlite_dir = os.path.dirname(sql_file_path)
                    sqlite_name = os.path.splitext(sqlite_base)[0].replace("_oracle", "")
                    potential_paths = [
                        os.path.join(sqlite_dir, f"{sqlite_name}.sqlite"),
                        os.path.join(sqlite_dir, f"{sqlite_name}.db"),
                        os.path.join(sqlite_dir, f"{sqlite_name}.sqlite3")
                    ]
                    for path in potential_paths:
                        if os.path.exists(path):
                            sqlite_path = path
                            break
                
                if sqlite_path:
                    success = load_performance_table(oracle_config, sqlite_path, table_name)
                else:
                    logger.error(f"Impossible de trouver le fichier SQLite source pour {table_name}")
                    success = False
            else:
                # Utiliser la méthode adaptée au type de table
                success = load_table_alternative(oracle_config, None, table_name, sql_file_path)
            
            results[table_name] = success
            
            if success:
                logger.info(f"Chargement alternatif réussi pour {table_name}")
            else:
                logger.warning(f"Échec du chargement alternatif pour {table_name}")
        
    except Exception as e:
        logger.error(f"Erreur lors du chargement alternatif: {str(e)}")
    
    return results

# Fonction principale pour recharger les tables manquantes depuis le rapport
def reload_missing_tables(report_output: str, oracle_config: Dict[str, str], sqlite_path: str) -> Dict[str, bool]:
    """
    Recharge les tables manquantes identifiées dans le rapport de validation.
    
    Args:
        report_output: Texte du rapport de validation
        oracle_config: Configuration Oracle
        sqlite_path: Chemin vers le fichier SQLite
        
    Returns:
        Résultats du rechargement
    """
    # Extraire les tables avec données manquantes du rapport
    tables_with_missing_data = []
    
    # Rechercher les sections du rapport
    missing_data_section = re.search(
        r"DÉTAILS DES DONNÉES MANQUANTES.*?Table: (.*?)\n.*?Lignes dans SQLite: (\d+).*?Lignes dans Oracle: (\d+)",
        report_output, re.DOTALL
    )
    
    if missing_data_section:
        table_name = missing_data_section.group(1).strip()
        sqlite_rows = int(missing_data_section.group(2))
        oracle_rows = int(missing_data_section.group(3))
        
        if oracle_rows < sqlite_rows:
            tables_with_missing_data.append(table_name)
            logger.info(f"Table identifiée avec données manquantes: {table_name} ({oracle_rows}/{sqlite_rows} lignes)")
    
    # Extraire les tables avec structure simplifiée
    simplified_tables_section = re.search(r"TABLES AVEC STRUCTURE SIMPLIFIÉE.*?- (.*?)$", report_output, re.MULTILINE)
    if simplified_tables_section:
        simplified_tables = re.findall(r"- (.*?)$", simplified_tables_section.group(0), re.MULTILINE)
        for table in simplified_tables:
            if table.strip() not in tables_with_missing_data:
                tables_with_missing_data.append(table.strip())
                logger.info(f"Table identifiée avec structure simplifiée: {table.strip()}")
    
    # Recharger les tables manquantes
    if tables_with_missing_data:
        logger.info(f"Tentative de rechargement pour {len(tables_with_missing_data)} tables problématiques")
        results = {}
        
        for table in tables_with_missing_data:
            logger.info(f"Rechargement de la table {table}")
            
            if "ON_TIME" in table.upper():
                # Utiliser le chargeur spécial pour les tables volumineuses
                success = load_performance_table(oracle_config, sqlite_path, table)
            else:
                # Pour les tables de lookup
                success = load_table_alternative(oracle_config, sqlite_path, table)
            
            results[table] = success
            
            if success:
                logger.info(f"Rechargement réussi pour {table}")
            else:
                logger.warning(f"Échec du rechargement pour {table}")
        
        return results
    
    else:
        logger.info("Aucune table problématique identifiée dans le rapport")
        return {}
