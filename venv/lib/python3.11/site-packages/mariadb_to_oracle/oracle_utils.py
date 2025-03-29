"""
Module pour interagir avec la base de données Oracle.
"""

import sys
import re
import oracledb
from typing import Dict, Optional, Tuple
from . import ORACLE_CONFIG, logger

def create_oracle_user(admin_config, new_username, new_password):
    """
    Crée un nouvel utilisateur Oracle avec les privilèges nécessaires.
    
    Args:
        admin_config: Configuration administrateur Oracle
        new_username: Nom du nouvel utilisateur
        new_password: Mot de passe du nouvel utilisateur
    """
    try:
        admin_conn = oracledb.connect(
            user=admin_config["user"],
            password=admin_config["password"],
            dsn=admin_config["dsn"]
        )
        cursor = admin_conn.cursor()
        try:
            cursor.execute(f"CREATE USER {new_username} IDENTIFIED BY {new_password}")
            logger.info(f"User '{new_username}' created.")
        except oracledb.DatabaseError as e:
            error, = e.args
            if "ORA-01920" in str(error) or "already exists" in str(error):
                logger.info(f"User '{new_username}' already exists; skipping creation.")
            else:
                raise
        
        # Accorder des privilèges adéquats
        cursor.execute(f"GRANT CONNECT, RESOURCE TO {new_username}")
        
        # Accorder un quota illimité sur le tablespace USERS pour résoudre l'erreur ORA-01950
        try:
            cursor.execute(f"ALTER USER {new_username} QUOTA UNLIMITED ON USERS")
            logger.info(f"Granted unlimited quota on USERS tablespace to {new_username}")
        except oracledb.DatabaseError as e:
            error, = e.args
            logger.warning(f"Could not grant tablespace quota: {error.message}")
            # Essayer de trouver les tablespaces disponibles
            try:
                cursor.execute("SELECT tablespace_name FROM dba_tablespaces")
                tablespaces = [row[0] for row in cursor.fetchall()]
                logger.info(f"Available tablespaces: {', '.join(tablespaces)}")
                # Essayer d'accorder un quota sur chaque tablespace disponible
                for ts in tablespaces:
                    try:
                        cursor.execute(f"ALTER USER {new_username} QUOTA UNLIMITED ON {ts}")
                        logger.info(f"Granted unlimited quota on {ts} tablespace to {new_username}")
                    except:
                        pass
            except:
                logger.warning("Could not retrieve available tablespaces")
        
        admin_conn.commit()
        cursor.close()
        admin_conn.close()
    except Exception as e:
        logger.error(f"Error creating Oracle user: {e}")
        sys.exit(1)

def execute_sql_file(new_user_config, sql_file_path, drop_tables=False):
    """
    Exécute un fichier SQL sur une base de données Oracle.
    
    Args:
        new_user_config: Configuration de l'utilisateur Oracle
        sql_file_path: Chemin vers le fichier SQL à exécuter
        drop_tables: Indique s'il faut supprimer les tables existantes
    """
    import re
    import datetime
    
    try:
        conn = oracledb.connect(
            user=new_user_config["user"],
            password=new_user_config["password"],
            dsn=new_user_config["dsn"]
        )
        cursor = conn.cursor()
        
        # Lire le fichier SQL et supprimer les backticks et les virgules incorrectes
        with open(sql_file_path, 'r') as f:
            sql_script = f.read()
            # Nettoyer le script SQL
            sql_script = sql_script.replace('`', '')
            # Supprimer toutes les clauses ON UPDATE
            sql_script = re.sub(r'\s+ON\s+UPDATE\s+CASCADE', '', sql_script, flags=re.IGNORECASE)
            sql_script = re.sub(r'\s+ON\s+UPDATE\s+SET\s+NULL', '', sql_script, flags=re.IGNORECASE)
            sql_script = re.sub(r'\s+ON\s+UPDATE\s+RESTRICT', '', sql_script, flags=re.IGNORECASE)
            sql_script = re.sub(r'\s+ON\s+UPDATE\s+NO\s+ACTION', '', sql_script, flags=re.IGNORECASE)
            sql_script = re.sub(r'\s+ON\s+UPDATE\s+SET\s+DEFAULT', '', sql_script, flags=re.IGNORECASE)
            # Corriger les virgules superflues
            sql_script = re.sub(r',\s*,', ',', sql_script)
            sql_script = re.sub(r'\(\s*,', '(', sql_script)
            sql_script = re.sub(r',\s*\)', ')', sql_script)
            sql_script = re.sub(r'^\s*,\s*', '', sql_script, flags=re.MULTILINE)
        
        # Analyse avancée pour détecter les ordres de création et d'insertion
        create_table_pattern = re.compile(r'CREATE TABLE (\w+)', re.IGNORECASE)
        insert_pattern = re.compile(r'INSERT INTO (\w+)', re.IGNORECASE)
        
        # Utiliser une meilleure expression régulière pour diviser les déclarations SQL
        statements = re.split(r';\s*$|\s*;\s*\n', sql_script, flags=re.MULTILINE)
        
        # Phase 1: Création des tables uniquement
        tables_created = set()
        oracle_objects = set()  # Pour suivre tous les objets Oracle existants
        
        # Vérifier quels objets existent déjà dans la base de données Oracle
        try:
            cursor.execute("""
                SELECT object_name 
                FROM user_objects 
                WHERE object_type IN ('TABLE', 'INDEX')
            """)
            for row in cursor:
                oracle_objects.add(row[0].upper())
        except Exception as e:
            logger.error(f"Erreur lors de la vérification des objets existants: {str(e)}")
        
        logger.info("Phase 1: Création des tables...")
        for stmt in statements:
            stmt = stmt.strip()
            if not stmt:
                continue
            
            # Détecter si c'est une instruction CREATE TABLE
            create_match = create_table_pattern.search(stmt)
            if create_match:
                table_name = create_match.group(1).upper()
                
                # Vérifier si la table existe déjà
                if table_name in oracle_objects:
                    logger.info(f"Table {table_name} existe déjà, utilisation de la table existante")
                    tables_created.add(table_name)
                    continue
                
                try:
                    # S'assurer que la requête est propre
                    stmt = stmt.replace('`', '')
                    # Supprimer toutes les clauses ON UPDATE
                    stmt = re.sub(r'\s+ON\s+UPDATE\s+CASCADE', '', stmt, flags=re.IGNORECASE)
                    stmt = re.sub(r'\s+ON\s+UPDATE\s+SET\s+NULL', '', stmt, flags=re.IGNORECASE)
                    stmt = re.sub(r'\s+ON\s+UPDATE\s+RESTRICT', '', stmt, flags=re.IGNORECASE)
                    stmt = re.sub(r'\s+ON\s+UPDATE\s+NO\s+ACTION', '', stmt, flags=re.IGNORECASE)
                    # Corriger les virgules superflues
                    stmt = re.sub(r',\s*,', ',', stmt)
                    stmt = re.sub(r'\(\s*,', '(', stmt)
                    stmt = re.sub(r',\s*\)', ')', stmt)
                    # Corriger la syntaxe déclaration des colonnes (enlever les virgules au début des lignes)
                    stmt = re.sub(r'(\n\s*),\s*', r'\1', stmt)
                    
                    # Vérifier si la requête contient des FOREIGN KEY avec des références à des tables non créées
                    fk_refs = re.findall(r'REFERENCES\s+(\w+)', stmt, re.IGNORECASE)
                    missing_tables = []
                    for ref in fk_refs:
                        ref_upper = ref.upper()
                        if ref_upper not in tables_created and ref_upper not in oracle_objects:
                            missing_tables.append(ref_upper)
                    
                    if missing_tables:
                        logger.warning(f"Table {table_name} fait référence à des tables non créées: {', '.join(missing_tables)}")
                        logger.info("Création de la table sans contraintes de clé étrangère...")
                        # Créer la table sans les contraintes FK
                        modified_stmt = re.sub(r',\s*(CONSTRAINT\s+\w+\s+)?FOREIGN KEY\s+\([^)]+\)\s+REFERENCES\s+\w+\s*\([^)]+\)(\s+ON\s+DELETE\s+\w+)?', '', stmt, flags=re.IGNORECASE)
                        cursor.execute(modified_stmt)
                        tables_created.add(table_name)
                        logger.info(f"Table {table_name} créée avec succès (sans FK)")
                        continue
                    
                    cursor.execute(stmt)
                    tables_created.add(table_name)
                    oracle_objects.add(table_name)  # Ajouter à la liste des objets
                    logger.info(f"Table {table_name} créée avec succès")
                except oracledb.DatabaseError as e:
                    error, = e.args
                    if error.code == 942:  # ORA-00942: table or view does not exist (référence à une table qui n'existe pas)
                        logger.error(f"Erreur de référence: {error.message}")
                        logger.info("Tentative de création sans contraintes de clé étrangère...")
                        # Créer la table sans les contraintes FK
                        modified_stmt = re.sub(r',\s*(CONSTRAINT\s+\w+\s+)?FOREIGN KEY\s+\([^)]+\)\s+REFERENCES\s+\w+\s*\([^)]+\)(\s+ON\s+DELETE\s+\w+)?', '', stmt, flags=re.IGNORECASE)
                        try:
                            cursor.execute(modified_stmt)
                            tables_created.add(table_name)
                            oracle_objects.add(table_name)
                            logger.info(f"Table {table_name} créée avec succès (sans FK)")
                        except Exception as e2:
                            logger.error(f"Échec de la création sans FK: {str(e2)}")
        
        # Phase 1.5: Ajouter les contraintes de clé étrangère après la création de toutes les tables
        logger.info("\nPhase 1.5: Ajout des contraintes de clé étrangère...")
        for stmt in statements:
            stmt = stmt.strip()
            if not stmt or not stmt.upper().startswith("CREATE TABLE"):
                continue
                
            create_match = create_table_pattern.search(stmt)
            if create_match:
                table_name = create_match.group(1).upper()
                if table_name not in tables_created:
                    continue
                
                # Extraire toutes les contraintes FK
                fk_constraints = re.findall(r'(CONSTRAINT\s+\w+\s+FOREIGN KEY\s+\([^)]+\)\s+REFERENCES\s+\w+\s*\([^)]+\)(\s+ON\s+DELETE\s+\w+)?)', stmt, re.IGNORECASE)
                for i, (fk_constraint, _) in enumerate(fk_constraints):
                    try:
                        # Vérifier si la contrainte fait référence à une table qui existe maintenant
                        ref_match = re.search(r'REFERENCES\s+(\w+)', fk_constraint, re.IGNORECASE)
                        if ref_match:
                            ref_table = ref_match.group(1).upper()
                            if ref_table in tables_created or ref_table in oracle_objects:
                                # Créer une déclaration ALTER TABLE pour ajouter la contrainte
                                alter_stmt = f"ALTER TABLE {table_name} ADD {fk_constraint}"
                                try:
                                    cursor.execute(alter_stmt)
                                    logger.info(f"Contrainte FK ajoutée à {table_name} référençant {ref_table}")
                                except oracledb.DatabaseError as e:
                                    error, = e.args
                                    # Ignorer si la contrainte existe déjà
                                    if error.code != 2275:  # ORA-02275: such a constraint already exists
                                        logger.error(f"Erreur lors de l'ajout de la contrainte FK: {error.message}")
                    except Exception as e:
                        logger.error(f"Erreur lors du traitement des contraintes FK: {str(e)}")
        
        # Après avoir créé toutes les tables, désactiver temporairement les contraintes
        logger.info("\nDésactivation temporaire des contraintes pour permettre le chargement des données...")
        try:
            cursor.execute("BEGIN DBMS_CONSTRAINT.DISABLE_ALL_CONSTRAINTS(); END;")
        except oracledb.DatabaseError as e:
            # Si le paquet n'existe pas, désactiver manuellement toutes les contraintes
            try:
                cursor.execute("""
                    BEGIN
                        FOR c IN (SELECT constraint_name, table_name FROM user_constraints WHERE constraint_type = 'R')
                        LOOP
                            BEGIN
                                EXECUTE IMMEDIATE 'ALTER TABLE ' || c.table_name || ' DISABLE CONSTRAINT ' || c.constraint_name;
                            EXCEPTION
                                WHEN OTHERS THEN NULL;
                            END;
                        END LOOP;
                    END;
                """)
                logger.info("Contraintes de clé étrangère désactivées")
            except Exception as e2:
                logger.warning(f"Impossible de désactiver automatiquement les contraintes: {str(e2)}")
                logger.info("Continuons avec les contraintes actives")
        
        # Phase 2: Exécution des insertions selon l'ordre logique des tables
        logger.info("\nPhase 2: Exécution des insertions de données...")
        
        # Ordre logique des tables pour l'insertion (indépendant des contraintes FK)
        table_order = [
            'PUBLISHERS',  # Tables principales indépendantes
            'JOBS',
            'STORES',
            'AUTHORS',
            'TITLES',      # Tables qui dépendent des tables principales 
            'EMPLOYEE',
            'PUB_INFO',
            'DISCOUNTS',
            'TITLEAUTHOR', # Tables qui dépendent de plusieurs autres tables
            'SALES',
            'ROYSCHED'     # Tables avec beaucoup de dépendances
        ]
        
        # Regrouper les insertions par table
        inserts_by_table = {}
        other_statements = []
        
        for stmt in statements:
            stmt = stmt.strip()
            if not stmt:
                continue
            
            # Vérifier si c'est un INSERT
            insert_match = insert_pattern.search(stmt)
            if insert_match:
                table_name = insert_match.group(1).upper()
                if table_name in tables_created:
                    if table_name not in inserts_by_table:
                        inserts_by_table[table_name] = []
                    inserts_by_table[table_name].append(stmt)
            else:
                other_statements.append(stmt)
        
        # Statistiques de suivi
        duplicate_count = 0
        insert_success_count = 0
        
        # Insérer les données dans les tables selon l'ordre défini
        for table_name in table_order:
            if table_name in inserts_by_table and table_name in tables_created:
                logger.info(f"Insertion des données dans la table {table_name}...")
                
                table_duplicate_count = 0
                table_success_count = 0
                
                for stmt in inserts_by_table[table_name]:
                    try:
                        # Traitement des dates pour les colonnes NOT NULL
                        if "date" in stmt.lower():
                            # Remplacer NULL par SYSDATE pour les colonnes de date
                            stmt = re.sub(r"(pubdate|hire_date|ord_date)([^,\)]*),\s*NULL", 
                                        r"\1\2, SYSDATE", stmt, flags=re.IGNORECASE)
                            
                            # Gérer les dates au format ISO
                            if re.search(r'\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}', stmt):
                                stmt = re.sub(r"'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})'",
                                            r"TO_DATE('\1', 'YYYY-MM-DD HH24:MI:SS')", stmt)
                            elif re.search(r'\d{4}-\d{2}-\d{2}', stmt):
                                stmt = re.sub(r"'(\d{4}-\d{2}-\d{2})'",
                                            r"TO_DATE('\1', 'YYYY-MM-DD')", stmt)
                        
                        cursor.execute(stmt)
                        table_success_count += 1
                        insert_success_count += 1
                    except oracledb.DatabaseError as e:
                        error, = e.args
                        # Traitement spécifique pour les violations de contrainte d'unicité
                        if error.code == 1:  # ORA-00001: unique constraint violated
                            table_duplicate_count += 1
                            duplicate_count += 1
                            if table_duplicate_count <= 3:  # Limiter l'affichage des messages pour éviter de spammer
                                # Extraire les détails de la violation
                                match = re.search(r"row with column values \(([^)]+)\) already exists", str(error))
                                if match:
                                    duplicate_values = match.group(1)
                                    logger.warning(f"Ignoré: Enregistrement dupliqué dans {table_name} avec {duplicate_values}")
                                else:
                                    logger.warning(f"Ignoré: Enregistrement dupliqué dans {table_name}")
                            elif table_duplicate_count == 4:
                                logger.info(f"D'autres doublons dans {table_name} seront ignorés silencieusement...")
                        elif error.code == 955:  # ORA-00955: name is already used by an existing object
                            logger.warning(f"Ignoré: Objet existe déjà (probablement index ou contrainte)")
                        else:
                            logger.error(f"Erreur Oracle lors de l'insertion dans {table_name} ({error.code}): {error.message}")
                            if error.code == 1400:  # NULL non autorisé
                                try:
                                    # Remplacer tous les NULL par SYSDATE dans les colonnes de date
                                    stmt = re.sub(r"(pubdate|hire_date|ord_date)([^,\)]*),\s*NULL", 
                                                r"\1\2, SYSDATE", stmt, flags=re.IGNORECASE)
                                    cursor.execute(stmt)
                                    table_success_count += 1
                                    insert_success_count += 1
                                except Exception as e2:
                                    logger.error(f"Échec de la correction: {str(e2)}")
                    except Exception as e:
                        logger.error(f"Erreur non-Oracle lors de l'insertion dans {table_name}: {str(e)}")
                
                # Afficher un résumé pour cette table
                if table_duplicate_count > 0:
                    logger.info(f"Résumé {table_name}: {table_success_count} insertion(s) réussie(s), {table_duplicate_count} doublon(s) ignoré(s)")
        
        # Affichage d'un message récapitulatif pour les opérations d'insertion
        logger.info(f"\nRésumé des insertions: {insert_success_count} réussie(s), {duplicate_count} doublon(s) ignoré(s)")
        
        # Exécuter les autres instructions (autres que INSERT et CREATE TABLE)
        logger.info("\nExécution des autres instructions...")
        other_execute_count = 0
        other_error_count = 0
        
        # Filtrer les autres instructions pour éviter les erreurs connues
        filtered_other_statements = []
        for stmt in other_statements:
            # Éliminer les instructions vides ou mal formatées
            if not stmt.strip() or stmt.strip() == ';':
                continue
                
            # Supprimer les points-virgules mal placés
            stmt = re.sub(r'\);+', ')', stmt)
            
            # Pour les instructions CREATE INDEX, vérifier si l'index existe déjà
            if stmt.upper().startswith('CREATE INDEX'):
                index_match = re.search(r'CREATE\s+INDEX\s+(\w+)', stmt, re.IGNORECASE)
                if index_match:
                    index_name = index_match.group(1).upper()
                    if index_name in oracle_objects:
                        continue  # Ignorer la création d'index si l'index existe déjà
            
            filtered_other_statements.append(stmt)
        
        # Exécuter les instructions filtrées
        for stmt in filtered_other_statements:
            try:
                cursor.execute(stmt)
                other_execute_count += 1
            except oracledb.DatabaseError as e:
                error, = e.args
                other_error_count += 1
                
                if error.code in (955, 1408, 904, 942, 1722):  # Codes d'erreur courants à ignorer
                    pass  # Ignorer silencieusement
                else:
                    logger.error(f"Erreur lors de l'exécution ({error.code}): {error.message}")
                    logger.error(f"Instruction problématique: {stmt[:100]}...")
            except Exception as e:
                other_error_count += 1
                logger.error(f"Erreur non-Oracle: {str(e)}")
        
        logger.info(f"Autres instructions: {other_execute_count} réussie(s), {other_error_count} erreur(s) ou ignorée(s)")
        
        # Réactiver les contraintes
        logger.info("\nRéactivation des contraintes...")
        try:
            cursor.execute("""
                BEGIN
                    FOR c IN (SELECT constraint_name, table_name FROM user_constraints WHERE constraint_type = 'R')
                    LOOP
                        BEGIN
                            EXECUTE IMMEDIATE 'ALTER TABLE ' || c.table_name || ' ENABLE CONSTRAINT ' || c.constraint_name;
                        EXCEPTION
                            WHEN OTHERS THEN NULL;
                        END;
                    END LOOP;
                END;
            """)
            logger.info("Contraintes de clé étrangère réactivées")
        except Exception as e:
            logger.warning(f"Impossible de réactiver automatiquement les contraintes: {str(e)}")
        
        # Validation finale
        conn.commit()
        logger.info("Exécution terminée avec succès.")
    except Exception as e:
        logger.error(f"Erreur générale lors de l'exécution du fichier SQL: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def get_sqlalchemy_uri(config):
    """
    Génère un URI SQLAlchemy à partir des paramètres de connexion Oracle.
    
    Args:
        config: Dictionnaire contenant les paramètres de connexion Oracle (user, password, dsn)
    
    Returns:
        str: URI SQLAlchemy pour se connecter à la base de données Oracle
    """
    username = config["user"]
    password = config["password"]
    dsn = config["dsn"]
    
    # Parser le DSN (format typique: host:port/service_name)
    dsn_parts = dsn.split('/')
    service_name = dsn_parts[1] if len(dsn_parts) > 1 else ''
    
    host_port = dsn_parts[0]
    host_port_parts = host_port.split(':')
    host = host_port_parts[0]
    port = host_port_parts[1] if len(host_port_parts) > 1 else '1521'  # Port Oracle par défaut
    
    # Construire l'URI SQLAlchemy
    uri = f"oracle+oracledb://{username}:{password}@{host}:{port}/{service_name}"
    return uri

def recreate_oracle_user(username: str, password: str, admin_config: Dict[str, str], force_recreate: bool = False) -> None:
    """
    Supprime et recrée l'utilisateur Oracle si force_recreate est True.
    
    Args:
        username: Nom de l'utilisateur Oracle à recréer
        password: Mot de passe de l'utilisateur
        admin_config: Configuration administrateur Oracle (user, password, dsn)
        force_recreate: Si True, l'utilisateur sera supprimé et recréé
    """
    if not force_recreate:
        return
        
    logger.info(f"Recréation de l'utilisateur {username} demandée...")
    
    try:
        admin_conn = oracledb.connect(
            user=admin_config["user"],
            password=admin_config["password"],
            dsn=admin_config["dsn"]
        )
        cursor = admin_conn.cursor()
        
        try:
            logger.info(f"Suppression de l'utilisateur {username} et de tous ses objets...")
            cursor.execute(f"DROP USER {username} CASCADE")
            logger.info(f"Utilisateur {username} supprimé avec succès")
        except oracledb.DatabaseError as e:
            error, = e.args
            if "ORA-01918" in str(error):  # Utilisateur n'existe pas
                logger.info(f"L'utilisateur {username} n'existe pas, création d'un nouvel utilisateur")
            else:
                logger.warning(f"Avertissement lors de la suppression: {error.message}")
        
        admin_conn.commit()
        cursor.close()
        admin_conn.close()
    except Exception as e:
        logger.warning(f"Avertissement pendant la récréation de l'utilisateur: {str(e)}")
        logger.warning("Poursuite du processus...")

def get_oracle_username_from_filepath(db_path: str) -> str:
    """
    Détermine un nom d'utilisateur Oracle valide à partir d'un chemin de fichier.
    
    Args:
        db_path: Chemin vers le fichier de base de données
        
    Returns:
        Un nom d'utilisateur Oracle valide
    """
    import os
    import re
    
    # Extraire le nom de base du fichier
    db_filename = os.path.basename(db_path)
    db_name = os.path.splitext(db_filename)[0]
    
    # Nettoyer pour Oracle (alphanumérique seulement)
    oracle_username = re.sub(r'[^a-zA-Z0-9]', '', db_name).lower()
    
    # S'assurer que le nom commence par une lettre (exigence Oracle)
    if not oracle_username or not oracle_username[0].isalpha():
        oracle_username = f"db{oracle_username}"
    
    # Limiter à 30 caractères (limitation Oracle)
    oracle_username = oracle_username[:30]
    
    return oracle_username

def display_sqlalchemy_info(user_config: Dict[str, str], print_example: bool = True) -> str:
    """
    Génère et affiche les informations de connexion SQLAlchemy.
    
    Args:
        user_config: Configuration utilisateur Oracle (user, password, dsn)
        print_example: Si True, affiche un exemple de code Python
        
    Returns:
        L'URI SQLAlchemy généré
    """
    sqlalchemy_uri = get_sqlalchemy_uri(user_config)
    
    logger.info("\nConnexion à la base de données via SQLAlchemy:")
    logger.info(f"URI: {sqlalchemy_uri}")
    
    if print_example:
        # On utilise print() pour s'assurer que ces informations sont toujours affichées,
        # même en mode silencieux (quiet)
        print("\nPour vous connecter à cette base de données avec SQLAlchemy, utilisez l'URI suivant:")
        print(f"SQLAlchemy URI: {sqlalchemy_uri}")
        print("\nExemple de code Python:")
        print(f"""
from sqlalchemy import create_engine

# Créer le moteur SQLAlchemy
engine = create_engine("{sqlalchemy_uri}")

# Utiliser le moteur pour les opérations de base de données
with engine.connect() as connection:
    result = connection.execute("SELECT * FROM <table_name>")
    for row in result:
        print(row)
""")
    
    return sqlalchemy_uri
