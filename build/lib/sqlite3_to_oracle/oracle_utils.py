"""
Module pour interagir avec la base de données Oracle.
"""

import sys
import re
import oracledb
from typing import Dict, Optional, Tuple
from . import ORACLE_CONFIG, logger

def check_oracle_connection(config: Dict[str, str]) -> Tuple[bool, str]:
    """
    Vérifie si la connexion à Oracle est possible avec les paramètres fournis.
    
    Args:
        config: Configuration Oracle contenant user, password et dsn
        
    Returns:
        Tuple contenant (réussite, message)
    """
    try:
        logger.debug(f"Tentative de connexion à Oracle avec l'utilisateur {config['user']}")
        conn = oracledb.connect(
            user=config["user"],
            password=config["password"],
            dsn=config["dsn"]
        )
        
        # Si on arrive ici, la connexion a réussi
        cursor = conn.cursor()
        
        # Récupérer quelques informations sur la BD
        cursor.execute("SELECT BANNER FROM V$VERSION")
        version_info = cursor.fetchone()[0]
        
        # Récupérer le nom de la base
        cursor.execute("SELECT SYS_CONTEXT('USERENV', 'DB_NAME') FROM DUAL")
        db_name = cursor.fetchone()[0]
        
        # Récupérer l'instance SID
        cursor.execute("SELECT SYS_CONTEXT('USERENV', 'INSTANCE_NAME') FROM DUAL")
        instance_name = cursor.fetchone()[0]
        
        # Récupérer les privilèges
        cursor.execute("SELECT * FROM SESSION_PRIVS")
        privileges = [row[0] for row in cursor.fetchall()]
        
        # Vérifier si l'utilisateur a le privilège CREATE SESSION (nécessaire pour toute connexion)
        has_create_session = "CREATE SESSION" in privileges
        
        # Ne pas vérifier CREATE USER pour tous les utilisateurs - uniquement important pour l'administrateur
        
        cursor.close()
        conn.close()
        
        if not has_create_session:
            return False, f"L'utilisateur {config['user']} n'a pas le privilège CREATE SESSION nécessaire pour se connecter."
        
        return True, f"Connexion réussie à Oracle ({version_info}) - Base: {db_name} - Instance: {instance_name}"
    
    except oracledb.DatabaseError as e:
        error, = e.args
        if "ORA-01017" in str(error):  # invalid username/password
            return False, f"Identifiants incorrects pour l'utilisateur {config['user']}"
        elif "ORA-12541" in str(error):  # no listener
            return False, f"Impossible de se connecter au serveur Oracle sur {config['dsn']}: le service n'est pas disponible"
        elif "ORA-12514" in str(error):  # service name not found
            return False, f"Service Oracle non trouvé: {config['dsn']}"
        else:
            return False, f"Erreur Oracle: {error.message} (code {error.code})"
    except Exception as e:
        return False, f"Erreur de connexion: {str(e)}"

def create_oracle_user(admin_config, new_username, new_password):
    """
    Crée un nouvel utilisateur Oracle avec les privilèges nécessaires.
    
    Args:
        admin_config: Configuration administrateur Oracle
        new_username: Nom du nouvel utilisateur
        new_password: Mot de passe du nouvel utilisateur
        
    Returns:
        bool: True si l'utilisateur a été créé ou existe déjà et est accessible
    """
    try:
        admin_conn = oracledb.connect(
            user=admin_config["user"],
            password=admin_config["password"],
            dsn=admin_config["dsn"]
        )
        cursor = admin_conn.cursor()
        user_created = False
        
        try:
            cursor.execute(f"CREATE USER {new_username} IDENTIFIED BY {new_password}")
            logger.info(f"User '{new_username}' created.")
            user_created = True
        except oracledb.DatabaseError as e:
            error, = e.args
            if "ORA-01920" in str(error) or "already exists" in str(error):
                logger.info(f"User '{new_username}' already exists; skipping creation.")
                user_created = True
            else:
                raise
        
        # Tentatives d'attribution de privilèges selon différentes approches
        privileges_granted = False
        
        # Approche 1: Essayer d'accorder le rôle CONNECT et RESOURCE
        try:
            cursor.execute(f"GRANT CONNECT, RESOURCE TO {new_username}")
            privileges_granted = True
            logger.info(f"Granted CONNECT, RESOURCE roles to {new_username}")
        except oracledb.DatabaseError as e1:
            logger.warning(f"Could not grant roles: {str(e1)}")
            
            # Approche 2: Essayer d'accorder les privilèges individuellement
            try:
                cursor.execute(f"GRANT CREATE SESSION TO {new_username}")
                cursor.execute(f"GRANT CREATE TABLE TO {new_username}")
                cursor.execute(f"GRANT CREATE VIEW TO {new_username}")
                cursor.execute(f"GRANT CREATE SEQUENCE TO {new_username}")
                privileges_granted = True
                logger.info(f"Granted individual privileges to {new_username}")
            except oracledb.DatabaseError as e2:
                logger.warning(f"Could not grant individual privileges: {str(e2)}")
        
        # Essayer d'accorder un quota sur les tablespaces
        tablespace_granted = False
        
        # Approche 1: Tablespace USERS
        try:
            cursor.execute(f"ALTER USER {new_username} QUOTA UNLIMITED ON USERS")
            tablespace_granted = True
            logger.info(f"Granted unlimited quota on USERS tablespace to {new_username}")
        except oracledb.DatabaseError as e3:
            logger.warning(f"Could not grant tablespace quota on USERS: {str(e3)}")
            
            # Approche 2: Essayer d'autres tablespaces communs
            for ts in ["DATA", "SYSTEM", "SYSAUX", "USER_DATA"]:
                try:
                    cursor.execute(f"ALTER USER {new_username} QUOTA UNLIMITED ON {ts}")
                    tablespace_granted = True
                    logger.info(f"Granted unlimited quota on {ts} tablespace to {new_username}")
                    break
                except:
                    pass
                    
            # Approche 3: Si rien n'a fonctionné, essayer de trouver tous les tablespaces
            if not tablespace_granted:
                try:
                    # Essayer différentes vues pour trouver les tablespaces
                    for view in ["DBA_TABLESPACES", "USER_TABLESPACES", "ALL_TABLESPACES"]:
                        try:
                            cursor.execute(f"SELECT tablespace_name FROM {view}")
                            tablespaces = [row[0] for row in cursor.fetchall()]
                            
                            for ts in tablespaces:
                                try:
                                    cursor.execute(f"ALTER USER {new_username} QUOTA UNLIMITED ON {ts}")
                                    tablespace_granted = True
                                    logger.info(f"Granted unlimited quota on {ts} tablespace to {new_username}")
                                    break
                                except:
                                    pass
                                    
                            if tablespace_granted:
                                break
                        except:
                            continue
                except Exception as e4:
                    logger.warning(f"Could not find or grant tablespace quota: {str(e4)}")
        
        # Approche de dernier recours: permission UNLIMITED TABLESPACE générale
        if not tablespace_granted:
            try:
                cursor.execute(f"GRANT UNLIMITED TABLESPACE TO {new_username}")
                tablespace_granted = True
                logger.info(f"Granted general UNLIMITED TABLESPACE privilege to {new_username}")
            except Exception as e5:
                logger.warning(f"Could not grant UNLIMITED TABLESPACE: {e5}")
                logger.warning("User may not be able to create tables due to quota issues")
        
        admin_conn.commit()
        cursor.close()
        admin_conn.close()
        
        # Vérifier que l'utilisateur peut se connecter
        logger.info(f"Vérification de la connexion pour l'utilisateur {new_username}...")
        test_config = {
            "user": new_username,
            "password": new_password,
            "dsn": admin_config["dsn"]
        }
        success, message = check_oracle_connection(test_config)
        
        if not success:
            logger.error(f"L'utilisateur {new_username} a été créé mais ne peut pas se connecter: {message}")
            logger.info("Tentative de résolution des problèmes de privilèges...")
            
            # Tenter de corriger les problèmes courants
            admin_conn = oracledb.connect(
                user=admin_config["user"],
                password=admin_config["password"],
                dsn=admin_config["dsn"]
            )
            cursor = admin_conn.cursor()
            
            # Vérifier et corriger les privilèges
            try:
                # S'assurer que l'utilisateur a un mot de passe valide
                cursor.execute(f"ALTER USER {new_username} IDENTIFIED BY {new_password}")
                
                # Accorder les privilèges nécessaires
                cursor.execute(f"GRANT CREATE SESSION TO {new_username}")
                cursor.execute(f"GRANT UNLIMITED TABLESPACE TO {new_username}")
                
                # Vérifier tous les tablespaces
                cursor.execute("SELECT TABLESPACE_NAME FROM USER_TABLESPACES")
                tablespaces = [row[0] for row in cursor.fetchall()]
                for ts in tablespaces:
                    try:
                        cursor.execute(f"ALTER USER {new_username} QUOTA UNLIMITED ON {ts}")
                        logger.debug(f"Accordé quota illimité sur {ts}")
                    except:
                        pass
                
                admin_conn.commit()
                logger.info("Privilèges supplémentaires accordés. Nouvelle tentative de connexion...")
                
                # Vérifier à nouveau
                success, message = check_oracle_connection(test_config)
                if success:
                    logger.info(f"Connexion réussie pour l'utilisateur {new_username} après correction")
                else:
                    logger.error(f"Échec persistant de connexion: {message}")
                    return False
            except Exception as e:
                logger.error(f"Erreur lors de la correction des privilèges: {str(e)}")
                return False
            finally:
                cursor.close()
                admin_conn.close()
        
        return success
    except Exception as e:
        logger.error(f"Error creating Oracle user: {e}")
        return False

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
        # Vérifier d'abord que la connexion fonctionne
        success, message = check_oracle_connection(new_user_config)
        if not success:
            error_msg = f"Impossible de se connecter à Oracle pour exécuter le script SQL: {message}"
            logger.error(error_msg)
            raise Exception(error_msg)
        
        conn = oracledb.connect(
            user=new_user_config["user"],
            password=new_user_config["password"],
            dsn=new_user_config["dsn"]
        )
        cursor = conn.cursor()
        
        # Désactiver auto-commit pour contrôler les transactions
        conn.autocommit = False
        
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
        
        # Améliorer la division des déclarations SQL en tenant compte des points-virgules dans les chaînes
        statements = []
        current_statement = ""
        in_string = False
        for line in sql_script.splitlines():
            line = line.strip()
            if not line or line.startswith('--'):
                continue
                
            # Ignorer les commentaires
            if line.startswith('/*') and '*/' in line:
                continue
                
            # Ajouter la ligne au statement actuel
            if current_statement:
                current_statement += " " + line
            else:
                current_statement = line
                
            # Gérer les points-virgules dans les chaînes
            for i, char in enumerate(line):
                if char == "'":
                    in_string = not in_string
                elif char == ';' and not in_string and i == len(line) - 1:
                    # Point-virgule à la fin de la ligne et pas dans une chaîne
                    statements.append(current_statement[:-1])  # Supprimer le point-virgule
                    current_statement = ""
                    break
                    
        # Ajouter le dernier statement s'il existe
        if current_statement:
            statements.append(current_statement)
        
        # Phase 1: Création des tables uniquement
        tables_created = set()
        oracle_objects = set()  # Pour suivre tous les objets Oracle existants
        missing_table_references = {}
        
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
        
        # Si drop_tables est True, supprimer les tables existantes
        if drop_tables:
            logger.info("Suppression des tables existantes demandée...")
            for table in oracle_objects:
                try:
                    cursor.execute(f"DROP TABLE {table} CASCADE CONSTRAINTS")
                    logger.debug(f"Table {table} supprimée")
                except Exception as e:
                    logger.warning(f"Erreur lors de la suppression de la table {table}: {str(e)}")
            conn.commit()
            oracle_objects.clear()
        
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
                    logger.debug(f"Table {table_name} existe déjà, utilisation de la table existante")
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
                        # Stocker les références manquantes pour un affichage groupé ultérieur
                        if table_name not in missing_table_references:
                            missing_table_references[table_name] = set()
                        missing_table_references[table_name].update(missing_tables)
                        
                        logger.debug(f"Table {table_name} fait référence à des tables non créées: {', '.join(missing_tables)}")
                        logger.debug("Création de la table sans contraintes de clé étrangère...")
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
        
        # Afficher un résumé des références manquantes 
        if missing_table_references:
            from .rich_logging import print_warning_message
            total_missing = sum(len(refs) for refs in missing_table_references.values())
            print_warning_message(f"{total_missing} références à des tables non créées détectées")
            logger.info("Résumé des références manquantes :")
            for table, refs in sorted(missing_table_references.items()):
                logger.info(f"  • Table {table} → {', '.join(sorted(refs))}")
            logger.info("Les contraintes de clé étrangère seront ajoutées dans une phase ultérieure")
        
        # Valider la transaction de création des tables
        conn.commit()
        
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
        
        # Valider les contraintes FK ajoutées
        conn.commit()
        
        # Après avoir créé toutes les tables, désactiver temporairement les contraintes pour permettre l'importation des données
        logger.info("\nDésactivation temporaire des contraintes pour permettre le chargement des données...")
        try:
            # Désactiver toutes les contraintes de clé étrangère
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
        except Exception as e:
            logger.warning(f"Impossible de désactiver automatiquement les contraintes: {str(e)}")
            logger.info("Continuons avec les contraintes actives")
        
        # Phase 2: Exécution des insertions
        logger.info("\nPhase 2: Exécution des insertions de données...")
        
        # Grouper les insertions par table pour un traitement ordonné
        inserts_by_table = {}
        
        # Parcourir toutes les déclarations pour trouver les INSERT
        for stmt in statements:
            stmt = stmt.strip()
            if not stmt:
                continue
            
            # Vérifier si c'est un INSERT INTO
            if stmt.upper().startswith('INSERT INTO'):
                insert_match = insert_pattern.search(stmt)
                if insert_match:
                    table_name = insert_match.group(1).upper()
                    if table_name in tables_created or table_name in oracle_objects:
                        if table_name not in inserts_by_table:
                            inserts_by_table[table_name] = []
                        inserts_by_table[table_name].append(stmt)
        
        # Statistiques pour suivre l'importation
        total_inserts = sum(len(stmts) for stmts in inserts_by_table.values())
        success_count = 0
        error_count = 0
        
        logger.info(f"Total d'insertions à traiter: {total_inserts}")
        
        # Exécuter les insertions pour chaque table
        for table_name, insert_statements in inserts_by_table.items():
            table_success = 0
            table_errors = 0
            
            logger.info(f"Insertion des données dans {table_name} ({len(insert_statements)} lignes)...")
            
            for stmt in insert_statements:
                try:
                    # Traiter les insertions de données SQLite pour Oracle
                    
                    # 1. Gérer les valeurs de date/heure
                    # Convertir les dates ISO en TO_DATE pour Oracle
                    date_pattern = r"'(\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?)'";
                    
                    # Remplacer les dates dans le format ISO par des appels TO_DATE d'Oracle
                    if re.search(date_pattern, stmt):
                        processed_stmt = re.sub(
                            r"'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'",
                            r"TO_DATE('\1', 'YYYY-MM-DD HH24:MI:SS')",
                            stmt
                        )
                        processed_stmt = re.sub(
                            r"'(\d{4}-\d{2}-\d{2})'",
                            r"TO_DATE('\1', 'YYYY-MM-DD')",
                            processed_stmt
                        )
                    else:
                        processed_stmt = stmt
                    
                    # 2. Gérer les valeurs NULL dans les colonnes DATE qui ne permettent pas NULL
                    if "NULL" in processed_stmt and "DATE" in table_name.upper():
                        # Remplacer NULL par SYSDATE pour les colonnes de date
                        processed_stmt = re.sub(
                            r"(date_column\s*[,\)])\s*NULL", 
                            r"\1 SYSDATE", 
                            processed_stmt, 
                            flags=re.IGNORECASE
                        )
                    
                    # 3. Gérer les problèmes de type booléen (SQLite utilise 0/1, Oracle TRUE/FALSE)
                    if re.search(r"[,\(]\s*(0|1)\s*[,\)]", processed_stmt):
                        # Remplacer 0 et 1 par des valeurs adaptées aux colonnes booléennes d'Oracle
                        processed_stmt = re.sub(r"([,\(]\s*)0(\s*[,\)])", r"\1'0'\2", processed_stmt)
                        processed_stmt = re.sub(r"([,\(]\s*)1(\s*[,\)])", r"\1'1'\2", processed_stmt)
                    
                    # Exécuter l'instruction SQL adaptée
                    cursor.execute(processed_stmt)
                    table_success += 1
                    success_count += 1
                
                except oracledb.DatabaseError as e:
                    error, = e.args
                    error_code = getattr(error, 'code', 'N/A')
                    error_message = getattr(error, 'message', str(error))
                    
                    # Gestion spécifique des erreurs courantes lors des insertions
                    if error_code == 1:  # ORA-00001: unique constraint violated
                        table_errors += 1
                        error_count += 1
                        logger.debug(f"Ignoré: Violation de contrainte unique dans {table_name}")
                    
                    elif error_code == 1400:  # ORA-01400: cannot insert NULL
                        # Essayer de remplacer NULL par une valeur par défaut appropriée
                        try:
                            # Remplacer les NULL par des valeurs par défaut appropriées selon le type de colonne
                            fixed_stmt = re.sub(r"([,\(]\s*)NULL(\s*[,\)])", r"\1''\2", processed_stmt)
                            cursor.execute(fixed_stmt)
                            table_success += 1
                            success_count += 1
                            logger.debug(f"Réparé: NULL remplacé par valeur par défaut dans {table_name}")
                        except Exception as e2:
                            table_errors += 1
                            error_count += 1
                            logger.debug(f"Échec de réparation de NULL: {str(e2)}")
                    
                    elif error_code == 1438:  # ORA-01438: value larger than specified precision
                        table_errors += 1
                        error_count += 1
                        logger.debug(f"Ignoré: Valeur trop grande pour la précision spécifiée dans {table_name}")
                    
                    else:
                        table_errors += 1
                        error_count += 1
                        logger.debug(f"Erreur {error_code} lors de l'insertion dans {table_name}: {error_message}")
                
                except Exception as e:
                    table_errors += 1
                    error_count += 1
                    logger.debug(f"Exception lors de l'insertion dans {table_name}: {str(e)}")
            
            # Afficher un résumé pour cette table
            logger.info(f"  → {table_name}: {table_success} insertion(s) réussie(s), {table_errors} erreur(s)")
            
            # Valider régulièrement les insertions pour éviter de perdre toutes les données en cas d'erreur
            conn.commit()
        
        # Afficher un résumé global des insertions
        logger.info(f"\nRésumé des insertions: {success_count} réussie(s), {error_count} échouée(s)")
        
        # Réactiver les contraintes après l'importation des données
        logger.info("\nRéactivation des contraintes...")
        try:
            # Réactiver toutes les contraintes de clé étrangère
            cursor.execute("""
                BEGIN
                    FOR c IN (SELECT constraint_name, table_name FROM user_constraints WHERE constraint_type = 'R' AND status = 'DISABLED')
                    LOOP
                        BEGIN
                            EXECUTE IMMEDIATE 'ALTER TABLE ' || c.table_name || ' ENABLE CONSTRAINT ' || c.constraint_name;
                        EXCEPTION
                            WHEN OTHERS THEN 
                                DBMS_OUTPUT.PUT_LINE('Erreur réactivation contrainte: ' || c.constraint_name);
                        END;
                    END LOOP;
                END;
            """)
            logger.info("Contraintes de clé étrangère réactivées")
        except Exception as e:
            logger.warning(f"Impossible de réactiver automatiquement les contraintes: {str(e)}")
        
        # Finaliser la transaction
        conn.commit()
        logger.info("\nImportation terminée avec succès")
    
    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
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
            error_code = getattr(error, 'code', 'N/A')
            error_message = getattr(error, 'message', str(error))
            
            if "ORA-01918" in str(error):  # Utilisateur n'existe pas
                logger.info(f"L'utilisateur {username} n'existe pas, création d'un nouvel utilisateur")
            elif "ORA-42299" in str(error):  # Erreur générique lors de la suppression d'un utilisateur
                from .rich_logging import print_warning_message
                print_warning_message(f"Problème lors de la suppression de l'utilisateur {username}")
                logger.warning(f"Erreur Oracle {error_code}: {error_message}")
                logger.info("Documentation Oracle: https://docs.oracle.com/error-help/db/ora-42299/")
                logger.info("Tentative de poursuite du processus...")
            else:
                from .rich_logging import print_warning_message
                print_warning_message(f"Avertissement lors de la suppression de l'utilisateur: {error_code}")
                logger.warning(f"Erreur Oracle: {error_message}")
        
        admin_conn.commit()
        cursor.close()
        admin_conn.close()
    except Exception as e:
        from .rich_logging import print_warning_message
        print_warning_message(f"Problème lors de la récréation de l'utilisateur {username}")
        logger.warning(f"Détail: {str(e)}")
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
    from .rich_logging import print_title, RICH_AVAILABLE
    
    sqlalchemy_uri = get_sqlalchemy_uri(user_config)
    
    logger.info("Connexion à la base de données via SQLAlchemy générée")
    
    if print_example:
        print_title("Informations de connexion SQLAlchemy")
        
        if RICH_AVAILABLE:
            try:
                from rich.syntax import Syntax
                from rich.console import Console
                from rich.panel import Panel
                
                console = Console()
                
                # Afficher l'URI
                console.print("[bold cyan]SQLAlchemy URI:[/bold cyan]")
                console.print(Panel(sqlalchemy_uri, expand=False, border_style="cyan"))
                
                # Afficher un exemple de code Python
                example_code = f"""from sqlalchemy import create_engine

# Créer le moteur SQLAlchemy
engine = create_engine("{sqlalchemy_uri}")

# Utiliser le moteur pour les opérations de base de données
with engine.connect() as connection:
    result = connection.execute("SELECT * FROM <table_name>")
    for row in result:
        print(row)
"""
                print("\n[bold cyan]Exemple de code Python:[/bold cyan]")
                syntax = Syntax(example_code, "python", theme="monokai", line_numbers=True)
                console.print(syntax)
                
            except ImportError:
                # Si les modules Rich nécessaires ne sont pas disponibles, fallback
                print("\nPour vous connecter à cette base de données avec SQLAlchemy, utilisez l'URI suivant:")
                print(f"SQLAlchemy URI: {sqlalchemy_uri}")
                print("\nExemple de code Python:")
                print(example_code)
        else:
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
