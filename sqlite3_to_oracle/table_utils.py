"""
Module contenant des utilitaires pour manipuler et corriger les structures de tables.
"""

import re
from typing import List, Tuple, Optional
from . import logger

def process_large_table(table_name: str, columns_def: str) -> str:
    """
    Traite spécifiquement les tables volumineuses avec beaucoup de colonnes.
    Implémente une approche plus robuste pour conserver le maximum de colonnes.
    
    Args:
        table_name: Nom de la table
        columns_def: Définition des colonnes de la table
        
    Returns:
        Instruction CREATE TABLE optimisée
    """
    logger.info(f"Optimisation de la table volumineuse {table_name}")
    
    # Extraire toutes les définitions de colonnes individuelles
    column_defs = []
    current_column = ""
    lines = columns_def.split('\n')
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Supprimer les virgules de fin et les commentaires
        line = re.sub(r',\s*$', '', line)
        line = re.sub(r'--.*$', '', line)
        
        # Si la ligne commence par un nom de colonne valide, c'est une nouvelle colonne
        if re.match(r'^\w+\s+[A-Za-z0-9]+', line):
            if current_column:
                column_defs.append(current_column.strip())
            current_column = line
        else:
            # Sinon, c'est une continuation de la définition précédente ou une contrainte
            current_column += " " + line
    
    # Ajouter la dernière colonne
    if current_column:
        column_defs.append(current_column.strip())
    
    # Nettoyer et valider chaque définition de colonne
    valid_columns = []
    primary_key_columns = []
    
    for column_def in column_defs:
        # Ignorer les contraintes au niveau de la table
        if re.match(r'^(CONSTRAINT|PRIMARY KEY|FOREIGN KEY|UNIQUE|CHECK)', column_def, re.IGNORECASE):
            # Si c'est une contrainte PRIMARY KEY, extraire les colonnes pour les ajouter plus tard
            pk_match = re.search(r'PRIMARY\s+KEY\s*\(\s*(.*?)\s*\)', column_def, re.IGNORECASE)
            if pk_match:
                pk_cols = pk_match.group(1).split(',')
                for col in pk_cols:
                    primary_key_columns.append(col.strip())
            continue
            
        # Extraire le nom et le type de la colonne
        match = re.match(r'^(\w+)\s+([A-Za-z0-9]+(?:\([^)]+\))?)(.*?)$', column_def, re.IGNORECASE | re.DOTALL)
        if not match:
            continue
            
        col_name, col_type, col_constraints = match.groups()
        
        # Valider et corriger le type
        if col_type.upper() in ('INTEGER', 'INT'):
            col_type = 'NUMBER'
        elif col_type.upper() == 'TEXT':
            col_type = 'VARCHAR2(4000)'
        elif col_type.upper() == 'REAL':
            col_type = 'NUMBER(38,10)'
        elif col_type.upper() == 'CHAR':
            col_type = 'VARCHAR2(1)'
        elif col_type.upper().startswith('VARCHAR'):
            # Extraire la taille entre parenthèses
            size_match = re.search(r'VARCHAR\((\d+)\)', col_type, re.IGNORECASE)
            if size_match:
                size = int(size_match.group(1))
                # Limiter à 4000 caractères max pour VARCHAR2
                if size > 4000:
                    size = 4000
                col_type = f"VARCHAR2({size})"
            else:
                col_type = 'VARCHAR2(255)'  # Valeur par défaut
        
        # Traiter les contraintes au niveau colonne
        col_constraints = col_constraints.strip()
        
        # Vérifier s'il y a PRIMARY KEY dans les contraintes de colonne
        if 'PRIMARY KEY' in col_constraints.upper():
            primary_key_columns.append(col_name)
            # Supprimer PRIMARY KEY de la définition de colonne
            col_constraints = re.sub(r'\bPRIMARY\s+KEY\b', '', col_constraints, flags=re.IGNORECASE).strip()
        
        # S'assurer que NOT NULL est présent si c'est une colonne PK
        if col_name in primary_key_columns and 'NOT NULL' not in col_constraints.upper():
            if col_constraints:
                col_constraints = col_constraints + " NOT NULL"
            else:
                col_constraints = "NOT NULL"
        
        # Reconstruire la définition de colonne nettoyée
        if col_constraints:
            clean_column = f"{col_name} {col_type} {col_constraints}"
        else:
            clean_column = f"{col_name} {col_type}"
            
        valid_columns.append(clean_column)
    
    # Créer la contrainte PRIMARY KEY si nécessaire
    pk_constraint = ""
    if primary_key_columns:
        # Limiter à une seule colonne PK pour éviter des problèmes
        pk_col = primary_key_columns[0]
        pk_constraint = f",\n  CONSTRAINT PK_{table_name[:10]} PRIMARY KEY ({pk_col})"
    
    # Construire la requête CREATE TABLE finale - SANS point-virgule final
    # (il sera ajouté plus tard)
    create_stmt = f"CREATE TABLE {table_name} (\n  "
    create_stmt += ",\n  ".join(valid_columns)
    create_stmt += pk_constraint
    create_stmt += "\n)"
    
    return create_stmt

def diagnose_and_fix_ora_00922(statement: str) -> str:
    """
    Diagnostique et corrige les erreurs ORA-00922 dans les instructions SQL.
    
    Args:
        statement: L'instruction SQL à corriger
        
    Returns:
        L'instruction SQL corrigée
    """
    logger.debug("Tentative de correction d'erreur ORA-00922...")
    
    # 1. Vérifier les contraintes mal formées
    create_match = re.match(r'CREATE TABLE\s+(\w+)\s*\((.*)\)', statement, re.DOTALL | re.IGNORECASE)
    if not create_match:
        return statement
        
    table_name = create_match.group(1)
    table_body = create_match.group(2)
    
    # Diviser en lignes et nettoyer
    lines = []
    for line in table_body.split('\n'):
        line = line.strip().rstrip(',')
        if line:
            lines.append(line)
    
    # Convertir les contraintes problématiques
    corrected_lines = []
    primary_key_added = False
    
    for line in lines:
        # Supprimer les contraintes non standard ou les convertir en syntaxe Oracle
        if re.search(r'CONSTRAINT\s+\w+\s+PRIMARY\s+KEY', line, re.IGNORECASE):
            # Extraire les colonnes de la clé primaire
            pk_cols_match = re.search(r'PRIMARY\s+KEY\s*\(\s*(.*?)\s*\)', line, re.IGNORECASE)
            if pk_cols_match:
                pk_cols = pk_cols_match.group(1)
                # Créer une contrainte PK avec un nom court (eviter la troncature)
                line = f"CONSTRAINT PK_{table_name[:10]} PRIMARY KEY ({pk_cols})"
                primary_key_added = True
        
        # Vérifier pour les colonnes avec PRIMARY KEY inline
        pk_column_match = re.match(r'(\w+)\s+([A-Za-z0-9]+(?:\([^)]+\))?)\s+PRIMARY\s+KEY', line, re.IGNORECASE)
        if pk_column_match:
            col_name = pk_column_match.group(1)
            col_type = pk_column_match.group(2)
            # Remplacer par une définition simple avec NOT NULL
            corrected_lines.append(f"{col_name} {col_type} NOT NULL")
            # Ajouter une contrainte PK séparée si pas déjà fait
            if not primary_key_added:
                corrected_lines.append(f"CONSTRAINT PK_{table_name[:10]} PRIMARY KEY ({col_name})")
                primary_key_added = True
            continue
        
        # Ajouter la ligne si elle n'a pas été traitée spécialement
        corrected_lines.append(line)
    
    # Reconstruire l'instruction avec des virgules entre les lignes
    corrected_body = ",\n  ".join(corrected_lines)
    corrected_statement = f"CREATE TABLE {table_name} (\n  {corrected_body}\n)"
    
    # Log les changements si différents
    if corrected_statement != statement:
        logger.debug("Instruction SQL corrigée pour ORA-00922")
    
    return corrected_statement

def sanitize_create_table_statement(statement: str) -> str:
    """
    Nettoie et corrige une déclaration CREATE TABLE pour Oracle.
    Traite en particulier les précisions numériques hors limites et les contraintes PRIMARY KEY.
    
    Args:
        statement: L'instruction CREATE TABLE à sanitiser
        
    Returns:
        L'instruction CREATE TABLE corrigée
    """
    # Vérifier si c'est une instruction CREATE TABLE
    if not statement.upper().startswith('CREATE TABLE'):
        return statement
    
    # Extraire le nom de la table 
    table_match = re.search(r'CREATE TABLE\s+(\w+)', statement, re.IGNORECASE)
    if not table_match:
        logger.warning(f"Format CREATE TABLE non reconnu: {statement[:100]}...")
        return statement
    
    table_name = table_match.group(1)
    
    # Extraire le corps de la table (tout ce qui est entre parenthèses)
    content_match = re.search(r'CREATE TABLE\s+\w+\s*\((.*)\);?', statement, re.DOTALL)
    if not content_match:
        logger.warning(f"Impossible d'extraire le contenu de la table: {statement[:100]}...")
        return statement
    
    columns_def = content_match.group(1)
    
    # Détecter si c'est une table volumineuse (> 30 colonnes) pour traitement spécial
    column_count = len(re.findall(r'\n\s*\w+\s+[A-Za-z0-9]+', columns_def, re.IGNORECASE))
    is_large_table = column_count > 30
    
    if is_large_table:
        logger.warning(f"Table volumineuse détectée ({table_name}) avec environ {column_count} colonnes - Traitement spécial")
        return process_large_table(table_name, columns_def)
    
    # Approche simplifiée pour les tables standard
    try:
        # Extraire toutes les définitions de colonnes (nom + type) sans les contraintes complexes
        column_matches = re.findall(r'\n\s*(\w+)\s+([A-Za-z0-9]+(?:\([0-9,]+\))?)', columns_def, re.IGNORECASE)
        
        if len(column_matches) > 0:
            # Construire une définition simple pour chaque colonne
            simple_columns = []
            id_column = None
            
            for col_name, col_type in column_matches:
                # Nettoyer le type et s'assurer qu'il est valide
                if col_type.upper() == 'CHAR':
                    col_type = 'VARCHAR2(255)'  # Convertir CHAR sans taille en VARCHAR2
                elif col_type.upper() == 'TEXT':
                    col_type = 'CLOB'
                elif col_type.upper() == 'INTEGER':
                    col_type = 'NUMBER'
                elif col_type.upper() == 'REAL':
                    col_type = 'NUMBER'
                
                # Vérifier si la colonne ressemble à une clé primaire
                if col_name.upper() in ('ID', 'CODE', f"{table_name.upper()}_ID") or "ID" in col_name.upper():
                    id_column = col_name
                    simple_columns.append(f"{col_name} {col_type} NOT NULL")
                else:
                    simple_columns.append(f"{col_name} {col_type}")
            
            # Construire la requête simplifiée
            simple_stmt = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(simple_columns)
            
            # Ajouter une contrainte PRIMARY KEY si un ID a été identifié
            if id_column:
                # Utiliser un nom court pour éviter la troncature
                simple_stmt += f",\n  CONSTRAINT PK_{table_name[:10]} PRIMARY KEY ({id_column})"
            
            simple_stmt += "\n)"
            
            # Vérifier et corriger les erreurs ORA-00922 potentielles
            return diagnose_and_fix_ora_00922(simple_stmt)
        
    except Exception as e:
        logger.debug(f"Erreur lors de la simplification de la structure de table: {str(e)}")
    
    # Méthode de secours: Utiliser une version ultra-simple
    ultra_simple = f"""CREATE TABLE {table_name} (
  id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  description VARCHAR2(4000)
)"""
    
    return ultra_simple

def analyze_table_structure(
    sqlite_path: str, 
    table_name: str
) -> Tuple[List[str], List[str], List[str], Optional[str]]:
    """
    Analyse la structure d'une table SQLite pour déterminer les colonnes appropriées
    pour Oracle et les contraintes de clé primaire.
    
    Args:
        sqlite_path: Chemin vers le fichier SQLite
        table_name: Nom de la table à analyser
        
    Returns:
        Tuple contenant (noms de colonnes, types Oracle, contraintes, colonne de clé primaire)
    """
    import sqlite3
    
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    
    # Récupérer les informations sur les colonnes
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns_info = cursor.fetchall()
    
    column_names = []
    oracle_types = []
    constraints = []
    primary_key_column = None
    
    for col in columns_info:
        col_id, col_name, col_type, not_null, default_val, is_pk = col
        
        # Convertir le type SQLite en type Oracle
        oracle_type = map_sqlite_to_oracle_type(col_type)
        
        column_names.append(col_name)
        oracle_types.append(oracle_type)
        
        # Déterminer les contraintes
        col_constraints = []
        
        if not_null:
            col_constraints.append("NOT NULL")
        
        if is_pk:
            primary_key_column = col_name
        
        if default_val is not None:
            # Adapter la valeur par défaut pour Oracle
            if default_val.upper() in ('NULL', 'CURRENT_TIMESTAMP'):
                col_constraints.append(f"DEFAULT {default_val}")
            elif col_type.upper() in ('INTEGER', 'INT', 'REAL', 'FLOAT', 'NUMERIC'):
                col_constraints.append(f"DEFAULT {default_val}")
            else:
                col_constraints.append(f"DEFAULT '{default_val}'")
        
        constraints.append(" ".join(col_constraints))
    
    cursor.close()
    conn.close()
    
    return column_names, oracle_types, constraints, primary_key_column

def map_sqlite_to_oracle_type(sqlite_type: str) -> str:
    """
    Convertit un type SQLite en type Oracle équivalent.
    
    Args:
        sqlite_type: Type SQLite à convertir
        
    Returns:
        Type Oracle équivalent
    """
    sqlite_type = sqlite_type.upper()
    
    # Retirer les parenthèses et paramètres pour obtenir le type de base
    base_type = re.sub(r'\(.*\)', '', sqlite_type).strip()
    
    # Mapper les types de base
    mapping = {
        'INTEGER': 'NUMBER',
        'INT': 'NUMBER',
        'BIGINT': 'NUMBER',
        'SMALLINT': 'NUMBER',
        'TINYINT': 'NUMBER',
        'REAL': 'NUMBER(38,10)',
        'FLOAT': 'NUMBER(38,10)',
        'DOUBLE': 'NUMBER(38,10)',
        'NUMERIC': 'NUMBER',
        'DECIMAL': 'NUMBER',
        'TEXT': 'VARCHAR2(4000)',
        'CHAR': 'CHAR(1)',
        'VARCHAR': 'VARCHAR2(255)',
        'BOOLEAN': 'NUMBER(1)',
        'DATE': 'DATE',
        'DATETIME': 'TIMESTAMP',
        'TIMESTAMP': 'TIMESTAMP',
        'BLOB': 'BLOB',
        'CLOB': 'CLOB'
    }
    
    # Si le type de base est dans notre mapping, on l'utilise
    if base_type in mapping:
        result_type = mapping[base_type]
        
        # Conserver les paramètres pour certains types
        if base_type in ('VARCHAR', 'CHAR', 'NUMERIC', 'DECIMAL'):
            param_match = re.search(r'\((.*)\)', sqlite_type)
            if param_match:
                params = param_match.group(1)
                
                # Ajuster les paramètres pour les limitations d'Oracle
                if base_type in ('VARCHAR'):
                    # VARCHAR a une limite de 4000 caractères dans Oracle
                    try:
                        size = int(params)
                        if size > 4000:
                            size = 4000
                        result_type = "VARCHAR2(" + str(size) + ")"
                    except:
                        result_type = "VARCHAR2(255)"
                elif base_type in ('CHAR'):
                    # CHAR a une limite de 2000 caractères dans Oracle
                    try:
                        size = int(params)
                        if size > 2000:
                            size = 2000
                        result_type = "CHAR(" + str(size) + ")"
                    except:
                        result_type = "CHAR(1)"
                elif base_type in ('NUMERIC', 'DECIMAL'):
                    # Vérifier la précision et l'échelle
                    if ',' in params:
                        precision, scale = params.split(',')
                        try:
                            precision = int(precision.strip())
                            scale = int(scale.strip())
                            
                            # Oracle limite la précision à 38 chiffres
                            if precision > 38:
                                precision = 38
                            
                            # L'échelle doit être inférieure ou égale à la précision
                            if scale > precision:
                                scale = precision
                                
                            result_type = "NUMBER(" + str(precision) + "," + str(scale) + ")"
                        except:
                            result_type = "NUMBER"
                    else:
                        try:
                            precision = int(params.strip())
                            if precision > 38:
                                precision = 38
                            result_type = "NUMBER(" + str(precision) + ")"
                        except:
                            result_type = "NUMBER"
        
        return result_type
    
    # Si on ne reconnaît pas le type, utiliser VARCHAR2 par défaut
    logger.warning(f"Type SQLite non reconnu: {sqlite_type}, utilisation de VARCHAR2 par défaut")
    return "VARCHAR2(255)"
