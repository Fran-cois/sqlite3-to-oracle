"""
Module spécialisé pour le chargement des tables volumineuses de type performances.
Traite notamment les tables avec beaucoup de colonnes et de lignes.
"""

import sqlite3
import oracledb
import re
import time
import os
from typing import Dict, List, Optional, Tuple, Any
from . import logger

def load_performance_table(
    oracle_config: Dict[str, str],
    sqlite_path: str, 
    table_name: str,
    use_varchar_for_decimals: bool = True,
    batch_size: int = 10000,
    enable_parallel: bool = True
) -> bool:
    """
    Fonction spécialisée pour charger la table de performance des vols.
    Cette fonction traite correctement les types décimaux/flottants et gère le grand volume de données.
    
    Args:
        oracle_config: Configuration Oracle
        sqlite_path: Chemin vers le fichier SQLite
        table_name: Nom de la table à charger
        use_varchar_for_decimals: Si True, utilise VARCHAR2 au lieu de NUMBER pour les décimaux
        batch_size: Nombre de lignes à traiter par lot
        enable_parallel: Activer le chargement parallèle si possible
        
    Returns:
        True si le chargement a réussi, False sinon
    """
    logger.info(f"Chargement spécialisé pour la table volumineuse {table_name}")
    
    try:
        # Connexion à Oracle
        oracle_conn = oracledb.connect(
            user=oracle_config["user"],
            password=oracle_config["password"],
            dsn=oracle_config["dsn"]
        )
        
        # Vérifier si le chargement parallèle est possible
        parallel_enabled = False
        if enable_parallel:
            try:
                cursor = oracle_conn.cursor()
                # Vérifier la version d'Oracle et les privilèges pour le chargement parallèle
                cursor.execute("SELECT BANNER FROM V$VERSION WHERE BANNER LIKE 'Oracle Database%'")
                version_info = cursor.fetchone()
                if version_info:
                    version_str = version_info[0]
                    match = re.search(r'(\d+)\.(\d+)\.', version_str)
                    if match and (int(match.group(1)) > 12 or (int(match.group(1)) == 12 and int(match.group(2)) >= 2)):
                        # Oracle 12.2+ supporte les hints ENABLE_PARALLEL_DML
                        cursor.execute("SELECT COUNT(*) FROM USER_SYS_PRIVS WHERE PRIVILEGE = 'CREATE TABLE'")
                        if cursor.fetchone()[0] > 0:
                            parallel_enabled = True
                            logger.info("Chargement parallèle activé pour améliorer les performances")
                cursor.close()
            except Exception as e:
                logger.debug(f"Impossible de vérifier le support parallèle: {str(e)}")
        
        # Déterminer si la table existe déjà et récupérer les colonnes existantes
        existing_columns = []
        try:
            cursor = oracle_conn.cursor()
            cursor.execute(f"SELECT column_name FROM user_tab_columns WHERE table_name = '{table_name.upper()}'")
            existing_columns = [row[0].upper() for row in cursor.fetchall()]
            cursor.close()
        except Exception as e:
            logger.debug(f"La table {table_name} n'existe pas encore ou erreur: {str(e)}")
        
        # Si la table n'existe pas, la créer avec les types adaptés
        if not existing_columns:
            # Connexion à SQLite pour récupérer la structure
            sqlite_conn = sqlite3.connect(sqlite_path)
            sqlite_cursor = sqlite_conn.cursor()
            
            # Récupérer la structure de la table
            sqlite_cursor.execute(f"PRAGMA table_info({table_name})")
            columns_info = sqlite_cursor.fetchall()
            
            # Préparer la création de la table Oracle
            create_cols = []
            primary_key_col = None
            
            for col in columns_info:
                col_name = col[1]
                col_type = col[2].upper()
                
                # Convertir les types SQLite vers Oracle
                if col_type in ('INTEGER', 'INT'):
                    oracle_type = 'NUMBER'
                elif col_type == 'TEXT':
                    oracle_type = 'VARCHAR2(4000)'
                elif 'DECIMAL' in col_type or 'FLOAT' in col_type or 'REAL' in col_type:
                    # Option pour utiliser VARCHAR2 pour tous les types décimaux problématiques
                    if use_varchar_for_decimals:
                        oracle_type = 'VARCHAR2(100)'  # Assez grand pour stocker des décimaux
                        logger.debug(f"Colonne {col_name}: utilisation de VARCHAR2 pour type {col_type}")
                    else:
                        # Sinon utiliser NUMBER avec une précision généreuse
                        oracle_type = 'NUMBER(38,10)'
                elif col_type.startswith('VARCHAR'):
                    # Extraire la taille
                    size_match = re.search(r'\((\d+)\)', col_type)
                    if size_match:
                        size = min(int(size_match.group(1)), 4000)
                        oracle_type = f"VARCHAR2({size})"
                    else:
                        oracle_type = 'VARCHAR2(255)'
                elif col_type == 'CHAR':
                    oracle_type = 'CHAR(1)'
                elif 'CHAR' in col_type:
                    # Extraire la taille pour CHAR(n)
                    size_match = re.search(r'\((\d+)\)', col_type)
                    if size_match:
                        size = min(int(size_match.group(1)), 4000)
                        oracle_type = f"VARCHAR2({size})"  # On utilise VARCHAR2 plutôt que CHAR
                    else:
                        oracle_type = 'VARCHAR2(30)'
                elif col_type == 'DATE' or col_type == 'DATETIME':
                    oracle_type = 'DATE'
                else:
                    oracle_type = 'VARCHAR2(255)'  # Type par défaut
                
                # Vérifier si c'est un candidat pour PRIMARY KEY
                is_pk = col[5] == 1  # SQLite: 5e colonne = pk flag
                not_null = col[3] == 1  # SQLite: 3e colonne = not null flag
                
                # Pour les colonnes potentielles de clé primaire
                if is_pk or (col_name.upper() in ('ID', f"{table_name.upper()}_ID") and not_null):
                    primary_key_col = col_name
                    create_cols.append(f"{col_name} {oracle_type} NOT NULL")
                else:
                    create_cols.append(f"{col_name} {oracle_type}")
            
            # Ajouter la contrainte PRIMARY KEY si trouvée
            pk_clause = ""
            if primary_key_col:
                pk_clause = f",\n  CONSTRAINT PK_{table_name[:20]} PRIMARY KEY ({primary_key_col})"
                
            # Construire et exécuter la requête CREATE TABLE
            create_stmt = "CREATE TABLE " + table_name + " (\n  "
            create_stmt += ",\n  ".join(create_cols)
            create_stmt += pk_clause
            create_stmt += "\n)"
            
            cursor = oracle_conn.cursor()
            try:
                cursor.execute(create_stmt)
                oracle_conn.commit()
                logger.info(f"Table {table_name} créée avec succès ({len(create_cols)} colonnes)")
            except Exception as create_error:
                logger.warning(f"Erreur lors de la création de la table: {str(create_error)}")
                
                # Si échec, essayer une approche plus simple
                try:
                    # Utiliser VARCHAR2 pour TOUTES les colonnes en cas d'échec
                    simplified_cols = []
                    for col in columns_info:
                        col_name = col[1]
                        # Utiliser VARCHAR2 pour tout sauf les colonnes qui doivent être numériques
                        if col_name.upper() in ('ID', 'YEAR', 'MONTH', 'DAYOFMONTH', 'DAYOFWEEK'):
                            simplified_cols.append(f"{col_name} NUMBER")
                        else:
                            simplified_cols.append(f"{col_name} VARCHAR2(4000)")
                        
                    # Créer la table simplifiée
                    simple_stmt = "CREATE TABLE " + table_name + " (\n  "
                    simple_stmt += ",\n  ".join(simplified_cols)
                    simple_stmt += "\n)"
                    
                    cursor.execute(simple_stmt)
                    oracle_conn.commit()
                    logger.info(f"Table {table_name} créée avec structure simplifiée (tout en VARCHAR2)")
                except Exception as e2:
                    logger.error(f"Échec de la création simplifiée: {str(e2)}")
                    return False
            
            # Mettre à jour les colonnes existantes
            cursor.execute(f"SELECT column_name FROM user_tab_columns WHERE table_name = '{table_name.upper()}'")
            existing_columns = [row[0].upper() for row in cursor.fetchall()]
            cursor.close()
            
            # Fermer la connexion SQLite
            sqlite_cursor.close()
            sqlite_conn.close()
        
        # Charger les données en lots avec mesure de performance
        start_time = time.time()
        sqlite_conn = sqlite3.connect(sqlite_path)
        sqlite_cursor = sqlite_conn.cursor()
        
        # Récupérer le nombre total de lignes pour le log
        sqlite_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows = sqlite_cursor.fetchone()[0]
        
        # Récupérer les noms des colonnes pour SQL
        sqlite_cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in sqlite_cursor.fetchall()]
        
        # Filtrer pour n'utiliser que les colonnes existantes dans Oracle
        columns_to_use = [col for col in columns if col.upper() in existing_columns]
        
        # Récupérer les types des colonnes pour adapter la conversion
        column_types = {}
        cursor = oracle_conn.cursor()
        cursor.execute(f"SELECT column_name, data_type FROM user_tab_columns WHERE table_name = '{table_name.upper()}'")
        for row in cursor.fetchall():
            column_types[row[0].upper()] = row[1]
        cursor.close()
        
        # Préparer l'instruction INSERT avec optimisation parallèle si possible
        insert_sql = f"INSERT"
        if parallel_enabled:
            insert_sql += "/*+ ENABLE_PARALLEL_DML */"
        insert_sql += f" INTO {table_name} ({', '.join(columns_to_use)}) VALUES ({', '.join([f':{i+1}' for i in range(len(columns_to_use))])})"
        
        # Optimiser la taille du lot en fonction du nombre de colonnes
        # Les tables avec beaucoup de colonnes devraient avoir des lots plus petits
        adaptive_batch_size = min(batch_size, max(1000, int(50000 / len(columns_to_use))))
        logger.info(f"Utilisation d'une taille de lot adaptative de {adaptive_batch_size} pour {len(columns_to_use)} colonnes")
        
        # Activer la gestion des erreurs par Oracle Array DML (si disponible)
        array_dml_errors_supported = False
        error_logging_table = f"{table_name}_ERRORS"
        
        try:
            if parallel_enabled:
                # Créer une table de log d'erreurs si possible
                cursor = oracle_conn.cursor()
                try:
                    # Tenter de supprimer la table d'erreurs si elle existe
                    cursor.execute(f"DROP TABLE {error_logging_table} PURGE")
                except:
                    pass
                
                try:
                    # Créer une nouvelle table de log d'erreurs
                    cursor.execute(f"""
                    CREATE TABLE {error_logging_table} (
                        ORA_ERR_NUMBER$ NUMBER,
                        ORA_ERR_MESG$ VARCHAR2(2000),
                        ORA_ERR_ROWID$ ROWID,
                        ORA_ERR_OPTYP$ VARCHAR2(2),
                        ORA_ERR_TAG$ VARCHAR2(2000),
                        ROW_ID NUMBER
                    )
                    """)
                    array_dml_errors_supported = True
                    logger.info(f"Table de log d'erreurs {error_logging_table} créée pour la gestion des erreurs")
                except Exception as e:
                    logger.debug(f"Impossible de créer la table de log d'erreurs: {str(e)}")
                
                cursor.close()
        except:
            pass
        
        # Chargement par lots
        total_inserted = 0
        skipped = 0
        
        # Log du début
        logger.info(f"Début du chargement pour {table_name} ({total_rows} lignes à traiter)")
        progress_interval = max(1, total_rows // 20)  # 5% du progrès
        
        # Utiliser l'API d'itération pour économiser la mémoire
        sqlite_cursor.execute(f"SELECT * FROM {table_name}")
        
        batch = []
        oracle_cursor = oracle_conn.cursor()
        
        # Optimiser pour Oracle
        oracle_cursor.arraysize = adaptive_batch_size
        
        for i, row in enumerate(sqlite_cursor):
            # Créer un tuple avec seulement les colonnes existantes dans Oracle
            filtered_row = []
            for j, col in enumerate(columns):
                if col.upper() in existing_columns:
                    value = row[j]
                    
                    # Traitement adapté au type cible dans Oracle
                    col_type = column_types.get(col.upper(), 'VARCHAR2')
                    
                    if value is None:
                        # NULL reste NULL quel que soit le type
                        filtered_row.append(None)
                    elif col_type == 'VARCHAR2' and isinstance(value, (float, int)):
                        # Pour les colonnes VARCHAR2, convertir les nombres en chaînes
                        filtered_row.append(str(value))
                    elif col_type.startswith('NUMBER') and isinstance(value, str) and value.strip():
                        # Pour les colonnes NUMBER, essayer de convertir les chaînes en nombres
                        try:
                            filtered_row.append(float(value))
                        except:
                            # Si la conversion échoue, laisser la valeur comme chaîne
                            filtered_row.append(value)
                    else:
                        # Utiliser la valeur telle quelle
                        filtered_row.append(value)
            
            # Ajouter la ligne au lot
            batch.append(filtered_row)
            
            # Insérer le lot lorsqu'il atteint la taille souhaitée
            if len(batch) >= adaptive_batch_size:
                try:
                    if array_dml_errors_supported:
                        # Utiliser LOG ERRORS pour capturer les erreurs sans interrompre le processus
                        modified_sql = f"{insert_sql} LOG ERRORS INTO {error_logging_table} ('BATCH_{i}') REJECT LIMIT UNLIMITED"
                        oracle_cursor.executemany(modified_sql, batch)
                    else:
                        oracle_cursor.executemany(insert_sql, batch)
                    
                    oracle_conn.commit()
                    total_inserted += len(batch)
                except Exception as e:
                    logger.warning(f"Échec d'insertion par lot: {e}")
                    # Essayer ligne par ligne
                    for row_idx, row_data in enumerate(batch):
                        try:
                            oracle_cursor.execute(insert_sql, row_data)
                            oracle_conn.commit()
                            total_inserted += 1
                        except Exception as row_e:
                            skipped += 1
                            if skipped < 10:  # Limiter le nombre de messages d'erreur
                                logger.debug(f"Échec d'insertion individuelle (ligne {i-len(batch)+row_idx+1}): {row_e}")
                
                # Vider le lot
                batch = []
                
                # Afficher le progrès
                if i % progress_interval == 0:
                    elapsed = time.time() - start_time
                    rows_per_sec = int(i / elapsed) if elapsed > 0 else 0
                    estimated_total = (elapsed / i) * total_rows if i > 0 else 0
                    estimated_remaining = estimated_total - elapsed
                    
                    logger.info(
                        f"Progression: {i}/{total_rows} lignes ({(i/total_rows)*100:.1f}%) - "
                        f"{rows_per_sec} lignes/sec - "
                        f"Temps restant estimé: {int(estimated_remaining/60)}:{int(estimated_remaining%60):02d}"
                    )
        
        # Traiter le dernier lot s'il y en a un
        if batch:
            try:
                if array_dml_errors_supported:
                    modified_sql = f"{insert_sql} LOG ERRORS INTO {error_logging_table} ('BATCH_FINAL') REJECT LIMIT UNLIMITED"
                    oracle_cursor.executemany(modified_sql, batch)
                else:
                    oracle_cursor.executemany(insert_sql, batch)
                
                oracle_conn.commit()
                total_inserted += len(batch)
            except Exception as e:
                logger.warning(f"Échec d'insertion du dernier lot: {e}")
                # Essayer ligne par ligne
                for row in batch:
                    try:
                        oracle_cursor.execute(insert_sql, row)
                        oracle_conn.commit()
                        total_inserted += 1
                    except:
                        skipped += 1
        
        # Vérifier les erreurs enregistrées si le log d'erreurs est activé
        if array_dml_errors_supported:
            try:
                cursor = oracle_conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {error_logging_table}")
                error_count = cursor.fetchone()[0]
                
                if error_count > 0:
                    logger.warning(f"{error_count} erreurs ont été enregistrées dans la table {error_logging_table}")
                    
                    # Récupérer quelques exemples d'erreurs pour aider au diagnostic
                    cursor.execute(f"SELECT ORA_ERR_NUMBER$, ORA_ERR_MESG$ FROM {error_logging_table} WHERE ROWNUM <= 5")
                    for err_num, err_msg in cursor:
                        logger.debug(f"Erreur Oracle {err_num}: {err_msg}")
                        
                    skipped += error_count
                
                cursor.close()
            except Exception as e:
                logger.debug(f"Impossible de vérifier la table de log d'erreurs: {str(e)}")
        
        # Fermer les curseurs et connexions
        oracle_cursor.close()
        oracle_conn.close()
        sqlite_cursor.close()
        sqlite_conn.close()
        
        # Calculer les statistiques de performance
        total_time = time.time() - start_time
        rows_per_second = int(total_inserted / total_time) if total_time > 0 else 0
        
        # Résumé final
        logger.info(
            f"Chargement terminé pour {table_name}: {total_inserted} lignes insérées, {skipped} lignes ignorées "
            f"en {total_time:.2f} secondes ({rows_per_second} lignes/sec)"
        )
        
        return total_inserted > 0
    
    except Exception as e:
        logger.error(f"Erreur lors du chargement de la table {table_name}: {str(e)}")
        return False
