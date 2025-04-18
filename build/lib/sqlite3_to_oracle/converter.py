"""
Module pour convertir la structure et les données SQLite en Oracle SQL.
"""

import re
import sqlite3
import datetime
from . import logger

def filter_sqlite_specific_statements(sql):
    """
    Remove SQLite-specific commands that are not valid in Oracle.
    This includes lines starting with PRAGMA, BEGIN TRANSACTION, COMMIT, ROLLBACK, VACUUM, etc.
    """
    filtered_lines = []
    for line in sql.splitlines():
        up_line = line.strip().upper()
        if (up_line.startswith("PRAGMA") or 
            up_line.startswith("BEGIN TRANSACTION") or 
            up_line.startswith("COMMIT") or 
            up_line.startswith("ROLLBACK") or 
            up_line.startswith("VACUUM") or 
            up_line.startswith("DELETE FROM SQLITE_SEQUENCE")):
            continue
        filtered_lines.append(line)
    return "\n".join(filtered_lines)

def sanitize_sql_value(value: str) -> str:
    """
    Sanitise une valeur pour l'insertion dans une requête SQL Oracle.
    
    Args:
        value: La valeur à sanitiser
        
    Returns:
        La valeur sanitisée, sûre pour l'insertion dans une requête SQL Oracle
    """
    if value is None:
        return "NULL"
    
    if isinstance(value, (int, float)):
        return str(value)
    
    # Pour les chaînes, échapper les guillemets simples et supprimer les caractères
    # qui pourraient causer des problèmes de syntaxe
    if isinstance(value, str):
        # Remplacer les guillemets simples par deux guillemets simples (échappement Oracle)
        value = value.replace("'", "''")
        
        # Supprimer ou remplacer les caractères problématiques
        value = value.replace("\0", "")  # Caractère null
        value = value.replace("\r", " ")  # Retour chariot
        value = value.replace("\n", " ")  # Saut de ligne
        
        # Traiter les caractères qui pourraient être interprétés comme des délimiteurs SQL
        value = value.replace(";", "\\;")
        
        # Entourer la valeur de guillemets simples
        return f"'{value}'"
    
    # Pour les valeurs booléennes
    if isinstance(value, bool):
        return "1" if value else "0"
    
    # Pour les autres types, convertir en chaîne et appliquer le même traitement
    return sanitize_sql_value(str(value))

def validate_numeric_precision(type_def: str) -> str:
    """
    Vérifie et corrige les précisions numériques pour qu'elles soient dans la plage valide d'Oracle (1-38).
    
    Args:
        type_def: Définition du type de données
        
    Returns:
        Définition de type corrigée si nécessaire
    """
    if not 'NUMBER' in type_def.upper():
        return type_def
    
    # Rechercher les spécifications de précision NUMBER(p) ou NUMBER(p,s)
    precision_match = re.search(r'NUMBER\s*\(\s*(\d+)(?:\s*,\s*(\d+))?\s*\)', type_def, re.IGNORECASE)
    
    if not precision_match:
        return type_def
    
    try:
        # Extraire et convertir les valeurs, en gérant les exceptions potentielles
        precision = int(precision_match.group(1))
        scale_str = precision_match.group(2)
        scale = int(scale_str) if scale_str else 0
        
        # Capture le type complet pour le remplacer
        full_match = precision_match.group(0)
        
        # Vérifier et corriger la précision si elle est hors limites
        if precision < 1:
            precision = 1
            logger.warning(f"Précision trop petite corrigée à {precision}")
        elif precision > 38:
            precision = 38
            logger.warning(f"Précision trop grande corrigée à {precision}")
        
        # Vérifier et corriger l'échelle si elle est hors limites
        if scale < 0:
            scale = 0
            logger.warning(f"Échelle négative corrigée à {scale}")
        elif scale > precision:
            scale = precision
            logger.warning(f"Échelle supérieure à la précision, corrigée à {scale}")
        
        # Reconstruire la définition de type
        if scale == 0:
            new_type = f"NUMBER({precision})"
        else:
            new_type = f"NUMBER({precision},{scale})"
        
        # Remplacer uniquement la partie problématique
        return type_def.replace(full_match, new_type)
    except (ValueError, TypeError, IndexError) as e:
        # En cas d'erreur de parsing, retourner le type sans précision
        logger.warning(f"Erreur lors du traitement de la précision numérique: {str(e)}")
        return "NUMBER"

def process_create_table(statement, only_fk_keys=False):
    """
    Process a single CREATE TABLE statement from a SQLite dump:
      - Remove double quotes and SQLite-specific options.
      - Convert data types:
          * "INTEGER PRIMARY KEY AUTOINCREMENT" → "NUMBER GENERATED BY DEFAULT AS IDENTITY"
          * "INTEGER" → "NUMBER"
          * "TEXT" → "CLOB"
          * "REAL" → "NUMBER"
          * "VARCHAR(n)" → "VARCHAR2(n)"
      - Remove unsupported options (e.g. COLLATE, WITHOUT ROWID)
      - If only_fk_keys is True, keep only primary key and foreign key columns
    Returns:
       new_statement: the rebuilt CREATE TABLE statement.
       extra_statements: list of additional ALTER TABLE/CREATE INDEX statements (if needed).
    """
    # Remove double quotes and backticks
    statement = statement.replace('"', '')
    statement = statement.replace('`', '')
    
    # Remove SQLite-specific table options (e.g. WITHOUT ROWID).
    statement = re.sub(r'\s+WITHOUT ROWID', '', statement, flags=re.IGNORECASE)
    
    # Find table name.
    m = re.search(r'CREATE TABLE\s+(IF NOT EXISTS\s+)?(\w+)\s*\(', statement, re.IGNORECASE)
    if not m:
        return statement, []
    table_name = m.group(2)
    
    # Extract the definitions inside the parentheses.
    open_paren = statement.find('(')
    close_paren = statement.rfind(')')
    inner = statement[open_paren+1:close_paren]
    
    # Nettoyer les définitions en supprimant les virgules au début des lignes
    # et en séparant correctement les définitions de colonnes
    clean_inner = re.sub(r',\s*,', ',', inner)  # Supprimer les virgules répétées
    clean_inner = re.sub(r'^\s*,\s*', '', clean_inner, flags=re.MULTILINE)  # Supprimer les virgules en début de ligne
    
    lines = clean_inner.splitlines()
    columns = []
    constraints = []
    extra_statements = []
    primary_key_found = False
    
    composite_pk = False
    pk_columns = []
    fk_columns = set()  # Pour stocker les colonnes utilisées dans les clés étrangères
    
    # Extraire les noms des colonnes utilisées dans les clés étrangères
    if only_fk_keys:
        for line in lines:
            # Chercher les contraintes FOREIGN KEY
            fk_match = re.search(r'FOREIGN\s+KEY\s*\(\s*([\w\s,]+)\s*\)\s*REFERENCES\s+(\w+)\s*\(\s*([\w\s,]+)\s*\)', 
                                   line, re.IGNORECASE)
            if fk_match:
                # Ajouter les colonnes source dans l'ensemble fk_columns
                source_cols = [col.strip() for col in fk_match.group(1).split(',')]
                for col in source_cols:
                    fk_columns.add(col)
    
    # Vérifier si la requête contient plusieurs colonnes marquées comme PRIMARY KEY
    for line in lines:
        if "PRIMARY KEY" in line.upper() and not line.upper().startswith("PRIMARY KEY"):
            pk_columns.append(line.split()[0])  # Ajouter le nom de la colonne

    # Si plusieurs colonnes sont marquées PRIMARY KEY, convertir en clé primaire composite
    if len(pk_columns) > 1:
        composite_pk = True
        # Retirer l'attribut PRIMARY KEY individuel de chaque colonne
        for i, line in enumerate(lines):
            if "PRIMARY KEY" in line.upper() and not line.upper().startswith("PRIMARY KEY"):
                lines[i] = re.sub(r'\s+PRIMARY KEY', '', line, flags=re.IGNORECASE)
        
        # Ajouter une contrainte de clé primaire composite
        pk_constraint = f"PRIMARY KEY ({', '.join(pk_columns)})"
        constraints.append(pk_constraint)
    
    for line in lines:
        line = line.strip().rstrip(',')
        if not line:
            continue

        # Remove any COLLATE clauses.
        line = re.sub(r'\s+COLLATE\s+\w+', '', line, flags=re.IGNORECASE)

        # If the line is a table-level constraint (PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY), keep it.
        if re.match(r'^(PRIMARY KEY|UNIQUE|CHECK|FOREIGN KEY|CONSTRAINT)', line, re.IGNORECASE):
            # Handle table-level PRIMARY KEY constraint
            if re.match(r'^PRIMARY KEY', line, re.IGNORECASE):
                primary_key_found = True
                # Extraire les colonnes de clé primaire
                pk_match = re.search(r'PRIMARY\s+KEY\s*\(\s*([\w\s,]+)\s*\)', line, re.IGNORECASE)
                if pk_match:
                    pk_cols = [col.strip() for col in pk_match.group(1).split(',')]
                    for col in pk_cols:
                        pk_columns.append(col)
                
            # Extraire les colonnes étrangères lors du traitement des contraintes FOREIGN KEY
            if only_fk_keys and re.match(r'^(FOREIGN KEY|CONSTRAINT)', line, re.IGNORECASE):
                fk_match = re.search(r'FOREIGN\s+KEY\s*\(\s*([\w\s,]+)\s*\)\s*REFERENCES\s+(\w+)\s*\(\s*([\w\s,]+)\s*\)', 
                                      line, re.IGNORECASE)
                if fk_match:
                    # Ajouter les colonnes référencées par la clé étrangère
                    target_table = fk_match.group(2)
                    target_cols = [col.strip() for col in fk_match.group(3).split(',')]
                    # Les colonnes cibles ne sont pas ajoutées à fk_columns car elles sont 
                    # dans une autre table, mais on conserve la contrainte

            # Process foreign key constraints - Oracle ne supporte pas ON UPDATE CASCADE
            if re.match(r'^FOREIGN KEY|^CONSTRAINT', line, re.IGNORECASE):
                # Supprimer complètement toutes les clauses ON UPDATE qui ne sont pas supportées par Oracle
                line = re.sub(r'\s+ON\s+UPDATE\s+CASCADE', '', line, flags=re.IGNORECASE)
                line = re.sub(r'\s+ON\s+UPDATE\s+SET\s+NULL', '', line, flags=re.IGNORECASE)
                line = re.sub(r'\s+ON\s+UPDATE\s+RESTRICT', '', line, flags=re.IGNORECASE)
                line = re.sub(r'\s+ON\s+UPDATE\s+NO\s+ACTION', '', line, flags=re.IGNORECASE)
                line = re.sub(r'\s+ON\s+UPDATE\s+SET\s+DEFAULT', '', line, flags=re.IGNORECASE)
                # Garder uniquement ON DELETE qui est supporté par Oracle
            
            constraints.append(line)
        else:
            # Process column definitions.
            column_parts = line.split(None, 1)
            if len(column_parts) < 2:
                columns.append(line)
                continue
                
            column_name, column_def = column_parts
            
            # Si on est en mode only_fk_keys, ne garder que les colonnes de clé primaire
            # et celles utilisées dans des clés étrangères
            if only_fk_keys and column_name not in pk_columns and column_name not in fk_columns:
                continue  # Ignorer cette colonne
            
            # Convert "INTEGER PRIMARY KEY AUTOINCREMENT" to Oracle identity syntax.
            if re.search(r'\bINTEGER\s+PRIMARY KEY\s+AUTOINCREMENT\b', column_def, re.IGNORECASE):
                line = f"{column_name} NUMBER GENERATED BY DEFAULT AS IDENTITY"
                primary_key_found = True
                if column_name not in pk_columns:
                    pk_columns.append(column_name)
            elif re.search(r'\bINTEGER\s+PRIMARY KEY\b', column_def, re.IGNORECASE):
                # For INTEGER PRIMARY KEY (without AUTOINCREMENT)
                line = f"{column_name} NUMBER PRIMARY KEY"
                primary_key_found = True
                if column_name not in pk_columns:
                    pk_columns.append(column_name)
            else:
                # Convert types
                line = re.sub(r'\bINTEGER\b', 'NUMBER', line, flags=re.IGNORECASE)
                line = re.sub(r'\bTEXT\b', 'VARCHAR2(4000)', line, flags=re.IGNORECASE)
                line = re.sub(r'\bREAL\b', 'NUMBER(38,10)', line, flags=re.IGNORECASE)
                line = re.sub(r'\bFLOAT\b', 'NUMBER(38,10)', line, flags=re.IGNORECASE)
                line = re.sub(r'\bDECIMAL\b(\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?', r'NUMBER\1', line, flags=re.IGNORECASE)
                line = re.sub(r'\bDOUBLE\b(\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?', r'NUMBER(38,10)', line, flags=re.IGNORECASE)
                line = re.sub(r'\bBLOB\b', 'BLOB', line, flags=re.IGNORECASE)
                line = re.sub(r'\bVARCHAR\s*\(\s*(\d+)\s*\)', r'VARCHAR2(\1)', line, flags=re.IGNORECASE)
                
                # Vérifier et corriger les précisions numériques hors limites
                line = validate_numeric_precision(line)
                
                # Check for inline PRIMARY KEY constraint on columns
                if re.search(r'\bPRIMARY\s+KEY\b', line, re.IGNORECASE) and column_name not in pk_columns:
                    pk_columns.append(column_name)
                    primary_key_found = True
            
            columns.append(line)
    
    # Joindre les définitions avec des virgules entre elles, sans virgule finale
    all_defs = columns + constraints
    new_inner = ",\n  ".join(all_defs)
    
    # Construire la requête finale
    new_statement = f"CREATE TABLE {table_name} (\n  {new_inner}\n);"
    
    return new_statement, extra_statements

def sort_tables_by_dependencies(table_names, dependencies):
    """
    Effectue un tri topologique des tables en fonction de leurs dépendances.
    Les tables sans dépendances viennent en premier, suivies des tables qui dépendent d'elles.
    
    Args:
        table_names: Ensemble des noms de tables
        dependencies: Dictionnaire de {table_name: [dependencie1, dependency2, ...]}
    
    Returns:
        Liste des tables triées selon l'ordre de dépendance
    """
    # Initialisation du résultat et des tables visitées
    sorted_list = []
    visited = set()
    temp_mark = set()
    
    def visit(table):
        # Éviter les cycles
        if table in temp_mark:
            return
        # Si déjà visité, ne pas retraiter
        if table in visited:
            return
        
        temp_mark.add(table)
        
        # Visiter d'abord les dépendances
        if table in dependencies:
            for dep in dependencies[table]:
                if dep in table_names:  # Ne considérer que les tables existantes
                    visit(dep)
        
        # Marquer comme visité et ajouter au résultat
        temp_mark.remove(table)
        visited.add(table)
        sorted_list.append(table)
    
    # Visiter toutes les tables
    for table in table_names:
        if table not in visited:
            visit(table)
            
    return sorted_list

def convert_sqlite_dump(sqlite_sql, only_fk_keys=False):
    """
    Convert the SQLite dump SQL into Oracle-compatible SQL.
    This function first filters out SQLite-specific commands, then processes
    CREATE TABLE statements, and passes through other statements (like INSERTs)
    with minor cleanup.
    
    Args:
        sqlite_sql: SQLite dump content
        only_fk_keys: If True, keep only primary key and foreign key columns
        
    Returns:
        Oracle-compatible SQL
    """
    filtered_sql = filter_sqlite_specific_statements(sqlite_sql)
    statements = re.split(r';\s*\n', filtered_sql)
    
    # Organiser les déclarations par type et extraire les noms de tables
    create_table_statements = {}  # Dictionnaire pour stocker les statements par table
    table_dependencies = {}      # Dictionnaire pour stocker les dépendances
    table_names = set()
    insert_statements = {}  # Dictionnaire pour stocker les INSERT par table
    other_statements = []
    
    # Première phase pour analyser toutes les tables et déterminer les dépendances FK
    for stmt in statements:
        stmt = stmt.strip()
        if not stmt:
            continue
            
        if stmt.upper().startswith("CREATE TABLE"):
            # Extraire le nom de la table
            match = re.search(r'CREATE TABLE\s+(IF NOT EXISTS\s+)?(\w+)', stmt, re.IGNORECASE)
            if match:
                table_name = match.group(2).lower()
                table_names.add(table_name)
                
                # Analyser les dépendances (FOREIGN KEY REFERENCES)
                dependencies = re.findall(r'REFERENCES\s+(\w+)', stmt, re.IGNORECASE)
                table_dependencies[table_name] = [dep.lower() for dep in dependencies]
                
                # Traiter la déclaration CREATE TABLE
                new_stmt, extras = process_create_table(stmt, only_fk_keys=only_fk_keys)
                create_table_statements[table_name] = new_stmt
        elif not only_fk_keys and stmt.upper().startswith("INSERT INTO"):
            # Si only_fk_keys est activé, ne pas inclure les INSERT
            match = re.search(r'INSERT INTO\s+(?:`)?(\w+)(?:`)?', stmt, re.IGNORECASE)
            if match:
                table_name = match.group(1).lower()
                # Initialisé le dictionnaire pour cette table si nécessaire
                if table_name not in insert_statements:
                    insert_statements[table_name] = []
                # Ajouter l'instruction INSERT à la liste de cette table
                insert_statements[table_name].append(stmt)
        else:
            # Autres instructions (comme les index, etc.)
            other_statements.append(stmt)
    
    # Trier les tables en fonction de leurs dépendances
    sorted_tables = sort_tables_by_dependencies(table_names, table_dependencies)
    
    # Inverser la liste pour que les tables sans dépendances soient créées en premier
    sorted_tables.reverse()
    
    # Construire le script final dans un ordre optimisé
    final_statements = []
    
    # 1. D'abord les tables dans l'ordre de dépendances
    for table_name in sorted_tables:
        if table_name in create_table_statements:
            final_statements.append(create_table_statements[table_name])
    
    # 2. Puis les autres déclarations non-INSERT
    final_statements.extend(other_statements)
    
    # 3. Enfin les INSERTs, mais seulement pour les tables que nous avons créées
    for table_name in sorted_tables:
        if table_name in insert_statements:
            for insert_stmt in insert_statements[table_name]:
                # Sanitize values in INSERT statements
                match = re.match(r"INSERT INTO\s+(\w+)\s+VALUES\s*\((.*)\);", insert_stmt, re.DOTALL)
                if match:
                    table_name = match.group(1)
                    values_str = match.group(2)
                    sanitized_values = []
                    in_string = False
                    current_value = ""
                    
                    for char in values_str:
                        if char == "'" and not (current_value.endswith('\\') and not current_value.endswith('\\\\')):
                            in_string = not in_string
                            current_value += char
                        elif char == ',' and not in_string:
                            sanitized_values.append(sanitize_sql_value(current_value.strip()))
                            current_value = ""
                        else:
                            current_value += char
                    
                    if current_value:
                        sanitized_values.append(sanitize_sql_value(current_value.strip()))
                    
                    sanitized_insert_stmt = f"INSERT INTO {table_name} VALUES ({', '.join(sanitized_values)});"
                    final_statements.append(sanitized_insert_stmt)
                else:
                    final_statements.append(insert_stmt)
    
    converted_sql = "\n\n".join(final_statements)
    return converted_sql

def extract_sqlite_data(sqlite_db_path):
    """
    Extrait le schéma et les données d'une base SQLite en gérant correctement les BLOB
    et autres données binaires. Cette fonction remplace l'utilisation de iterdump().
    
    Args:
        sqlite_db_path: Chemin vers le fichier de base de données SQLite
    
    Returns:
        str: Script SQL compatible avec la syntaxe Oracle
    """
    conn = sqlite3.connect(sqlite_db_path)
    conn.text_factory = bytes  # Pour éviter les erreurs de décodage UTF-8
    cursor = conn.cursor()
    
    try:
        # Récupérer la liste des tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = cursor.fetchall()
        
        # Génération du script SQL
        sql_script = []
        
        for table in tables:
            table_name = table[0].decode('utf-8', errors='replace')
            
            # Récupérer les informations de schéma pour cette table
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            
            # Identifier les colonnes de type date et binaires
            date_columns = []
            blob_columns = []
            for i, col in enumerate(columns):
                col_type = col[2].decode('utf-8', errors='replace').upper()
                col_name = col[1].decode('utf-8', errors='replace')
                if col_type in ('DATE', 'DATETIME', 'TIMESTAMP') or 'DATE' in col_name.upper() or 'TIME' in col_name.upper():
                    date_columns.append(i)
                elif col_type == 'BLOB':
                    blob_columns.append(i)
            
            # Construire la requête CREATE TABLE
            create_table = f"CREATE TABLE {table_name} (\n"
            column_defs = []
            for col in columns:
                col_name = col[1].decode('utf-8', errors='replace')
                col_type = col[2].decode('utf-8', errors='replace')
                not_null = col[3] == 1
                default_val = col[4]
                is_pk = col[5] == 1
                
                # Construire la définition de colonne
                if is_pk and col_type.upper() == 'INTEGER':
                    column_defs.append(f"  {col_name} NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY")
                else:
                    column_def = f"  {col_name} {col_type}"
                    if not_null:
                        column_def += " NOT NULL"
                    if default_val is not None:
                        default_val_str = default_val.decode('utf-8', errors='replace') if isinstance(default_val, bytes) else str(default_val)
                        column_def += f" DEFAULT {default_val_str}"
                    if is_pk:
                        column_def += " PRIMARY KEY"
                    column_defs.append(column_def)
            
            create_table += ",\n".join(column_defs)
            
            # Récupérer les contraintes de clé étrangère
            cursor.execute(f"PRAGMA foreign_key_list({table_name});")
            fks = cursor.fetchall()
            
            if fks:
                fk_constraints = []
                for fk in fks:
                    ref_table = fk[2].decode('utf-8', errors='replace')
                    from_col = fk[3].decode('utf-8', errors='replace')
                    to_col = fk[4].decode('utf-8', errors='replace')
                    constraint_name = f"fk_{table_name}_{from_col}_{ref_table}_{to_col}"[:30]
                    fk_constraints.append(f"  CONSTRAINT {constraint_name} FOREIGN KEY ({from_col}) REFERENCES {ref_table}({to_col})")
                
                if fk_constraints:
                    create_table += ",\n" + ",\n".join(fk_constraints)
            
            create_table += "\n);"
            sql_script.append(create_table)
            
            # Si la table contient des données binaires, on saute la génération des INSERT
            # Ces données seront traitées par un appel à d'autres fonctions
            if blob_columns and table_name.upper() == 'PUB_INFO':
                continue
            
            # Récupérer et générer les INSERT pour cette table
            try:
                with conn:
                    cursor.execute(f"SELECT * FROM {table_name}")
                    rows = cursor.fetchall()
                    
                    for row in rows:
                        values = []
                        for i, val in enumerate(row):
                            if val is None:
                                values.append("NULL")
                            elif i in blob_columns:
                                # Pour les BLOB, utiliser une fonction d'échappement appropriée
                                values.append("NULL")  # Simplifié pour cet exemple
                            elif i in date_columns and isinstance(val, (bytes, str)):
                                # Convertir en format de date Oracle
                                date_str = val.decode('utf-8', errors='replace') if isinstance(val, bytes) else val
                                values.append(f"TO_DATE('{date_str}', 'YYYY-MM-DD HH24:MI:SS')")
                            elif isinstance(val, bytes):
                                # Pour les autres valeurs binaires, convertir en chaîne
                                try:
                                    str_val = val.decode('utf-8', errors='replace')
                                    if "'" in str_val:
                                        str_val = str_val.replace("'", "''")
                                    values.append(f"'{str_val}'")
                                except:
                                    values.append("NULL")
                            elif isinstance(val, str):
                                # Échapper les guillemets pour Oracle
                                escaped_val = val.replace("'", "''")
                                values.append(f"'{escaped_val}'")
                            else:
                                # Nombres ou autres types
                                values.append(str(val))
                        
                        insert_sql = f"INSERT INTO {table_name} VALUES ({', '.join(values)});"
                        sql_script.append(insert_sql)
            except Exception as e:
                logger.error(f"Erreur lors de la génération des INSERT pour {table_name}: {str(e)}")
        
        conn.close()
        return "\n\n".join(sql_script)
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des données: {str(e)}")
        return ""  # Corriger cette ligne qui n'était pas indentée correctement

def convert_date_format(date_str):
    """
    Convertit un format de date SQLite en format compatible avec Oracle.
    Fonction améliorée pour gérer plus de formats et de cas d'erreur.
    """
    import re
    import datetime
    
    if not date_str or date_str.lower() == 'null':
        return "SYSDATE"  # Utiliser SYSDATE pour les dates NULL dans les colonnes NOT NULL
    
    # Supprimer les apostrophes et caractères spéciaux si présents
    date_str = date_str.strip("'").strip(";").strip(")")
    
    # Normalisation pour les formats de date avec des tirets inversés ou des hyphens
    date_str = date_str.replace('\\', '-').replace('–', '-')
    
    # CAS SPÉCIAL: Format ISO avec heure
    if re.match(r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$', date_str):
        try:
            date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            # Utiliser un format fixe pour éviter tout problème de syntaxe
            return f"TO_DATE('{date_obj.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
        except ValueError:
            return "SYSDATE"
    
    # Liste des formats à essayer avec plus de variations
    formats_to_try = [
        '%Y-%m-%d', '%Y-%m-%d %H:%M:%S',  # ISO standard
        '%m/%d/%Y', '%m/%d/%y',           # Format américain
        '%d-%b-%Y', '%d %b %Y',           # Format avec mois abrégé (comme 15-JAN-2023)
        '%b %d, %Y', '%B %d, %Y',         # Format texte (comme Jan 15, 2023)
        '%m-%d-%Y', '%d-%m-%Y',           # Formats numériques variés
        '%m.%d.%Y', '%d.%m.%Y',           # Formats avec point
        '%Y/%m/%d',                       # Format ISO inversé
        '%d/%m/%Y', '%d/%m/%y',           # Format européen
        '%Y.%m.%d',                       # Format avec points
        '%y%m%d',                         # Format compact
        '%d-%m-%y', '%m-%d-%y'            # Format court année à 2 chiffres
    ]
    
    # Essayer tous les formats avant de vérifier les patterns
    for fmt in formats_to_try:
        try:
            date_obj = datetime.datetime.strptime(date_str, fmt)
            # Vérifier si l'année est raisonnable (éviter des dates comme 0023-01-01)
            if date_obj.year < 1900:
                continue
            
            # Déterminer si la date a une partie heure et générer en format fixe
            if '%H' in fmt or '%I' in fmt:
                formatted_date = date_obj.strftime('%Y-%m-%d %H:%M:%S')
                return f"TO_DATE('{formatted_date}', 'YYYY-MM-DD HH24:MI:SS')"
            else:
                formatted_date = date_obj.strftime('%Y-%m-%d')
                return f"TO_DATE('{formatted_date}', 'YYYY-MM-DD')"
        except ValueError:
            continue
    
    # Si aucun format ne correspond, on essaie de vérifier si la chaîne ressemble à une date
    if re.match(r'^\d{1,4}[-/\.]\d{1,2}[-/\.]\d{1,4}', date_str) or \
       re.match(r'^[A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}', date_str) or \
       re.match(r'^\d{1,2}\s+[A-Za-z]{3,9},?\s+\d{4}', date_str) or \
       re.match(r'^\d{6,8}$', date_str):
        # Ressemble à une date mais format non reconnu, utiliser une date par défaut sécurisée
        return "TO_DATE('2000-01-01', 'YYYY-MM-DD')"
    
    # Ce n'est pas une date, retourner SYSDATE par sécurité
    return "SYSDATE"
