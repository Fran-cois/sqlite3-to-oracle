"""
Module pour charger les tables de lookup (référentielles) depuis SQLite vers Oracle.
Gère le cas particulier des tables L_* qui ont généralement une structure simple.
"""

import re
import oracledb
from typing import Dict, List, Optional
from . import logger

def create_simplified_lookup_table(
    oracle_conn: oracledb.Connection,
    table_name: str,
    key_col_name: str = "Code",
    value_col_name: str = "Description"
) -> bool:
    """
    Crée une table de lookup simple avec une structure standard.
    
    Args:
        oracle_conn: Connexion Oracle
        table_name: Nom de la table à créer
        key_col_name: Nom de la colonne clé
        value_col_name: Nom de la colonne valeur
        
    Returns:
        True si la création a réussi, False sinon
    """
    cursor = oracle_conn.cursor()
    
    try:
        # Déterminer le type de la colonne clé en fonction du nom
        key_type = "VARCHAR2(255)"
        if key_col_name.upper() in ('ID', 'CODE_ID', 'SEQ', 'NUM'):
            key_type = "NUMBER"
        elif key_col_name.upper() in ('CODE') and 'AIRPORT' not in table_name.upper():
            key_type = "VARCHAR2(30)"
        elif 'AIRPORT' in table_name.upper() or 'STATE' in table_name.upper():
            key_type = "CHAR(3)"
        
        # Structure simple mais efficace pour les tables de lookup
        create_stmt = f"""
        CREATE TABLE {table_name} (
          {key_col_name} {key_type} NOT NULL,
          {value_col_name} VARCHAR2(4000),
          CONSTRAINT PK_{table_name[:10]} PRIMARY KEY ({key_col_name})
        )
        """
        
        cursor.execute(create_stmt)
        oracle_conn.commit()
        logger.info(f"Table de lookup {table_name} créée avec succès")
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la création de la table de lookup {table_name}: {str(e)}")
        return False
    finally:
        cursor.close()

def parse_and_load_lookup_data(
    oracle_conn: oracledb.Connection,
    sql_file_path: str,
    table_name: str
) -> bool:
    """
    Parse un fichier SQL pour extraire et charger les données de lookup.
    
    Args:
        oracle_conn: Connexion Oracle
        sql_file_path: Chemin vers le fichier SQL
        table_name: Nom de la table à charger
        
    Returns:
        True si le chargement a réussi, False sinon
    """
    cursor = oracle_conn.cursor()
    
    try:
        # Lire le fichier SQL
        with open(sql_file_path, 'r') as f:
            sql_content = f.read()
        
        # Rechercher toutes les insertions pour cette table
        insert_pattern = rf"INSERT INTO\s+{table_name}\s+VALUES\s*\((.*?)\);?"
        inserts = re.findall(insert_pattern, sql_content, re.IGNORECASE | re.DOTALL)
        
        if not inserts:
            logger.warning(f"Aucune donnée trouvée pour la table {table_name}")
            return False
        
        # Créer la table avec une structure simplifiée si elle n'existe pas
        try:
            cursor.execute(f"SELECT 1 FROM {table_name} WHERE ROWNUM = 1")
        except:
            create_simplified_lookup_table(oracle_conn, table_name)
        
        # Insérer les données
        rows_inserted = 0
        for values in inserts:
            # Extraire les valeurs individuelles
            raw_values = []
            current_val = ""
            in_quotes = False
            
            for char in values:
                if char == "'" and (len(current_val) == 0 or current_val[-1] != '\\'):
                    in_quotes = not in_quotes
                    current_val += char
                elif char == ',' and not in_quotes:
                    raw_values.append(current_val.strip())
                    current_val = ""
                else:
                    current_val += char
            
            if current_val:
                raw_values.append(current_val.strip())
            
            # S'assurer qu'il y a au moins 2 valeurs (code et description)
            if len(raw_values) >= 2:
                try:
                    # Oracle a besoin de guillemets doubles pour les identifiants
                    cursor.execute(
                        f"INSERT INTO {table_name} (Code, Description) VALUES (:1, :2)",
                        [raw_values[0].strip("'"), raw_values[1].strip("'")]
                    )
                    oracle_conn.commit()
                    rows_inserted += 1
                except Exception as insert_error:
                    logger.debug(f"Erreur d'insertion: {str(insert_error)}")
        
        logger.info(f"Chargement réussi pour {table_name}: {rows_inserted} lignes insérées")
        return rows_inserted > 0
    except Exception as e:
        logger.error(f"Erreur lors du chargement des données pour {table_name}: {str(e)}")
        return False
    finally:
        cursor.close()
