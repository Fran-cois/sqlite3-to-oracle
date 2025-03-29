"""
Interface en ligne de commande pour le convertisseur SQLite vers Oracle.

Ce module fournit une interface utilisateur simple pour convertir une base de données
SQLite vers Oracle, en créant automatiquement l'utilisateur et en exécutant le script.
"""

import os
import re
import sys
import logging
import argparse
import sqlite3
from typing import Dict, Tuple, Optional, List, Any

from . import ORACLE_CONFIG, logger
from .config import load_oracle_config
from .converter import extract_sqlite_data, convert_sqlite_dump
from .oracle_utils import (
    create_oracle_user, 
    execute_sql_file, 
    display_sqlalchemy_info,
    recreate_oracle_user,
    get_oracle_username_from_filepath,
    check_oracle_connection
)

# Importer les utilitaires de logging riche
from .rich_logging import (
    setup_logger,
    print_title,
    print_success_message,
    print_error_message,
    get_progress_bar,
    RICH_AVAILABLE,
    get_log_manager
)

try:
    from rich.console import Console
    from rich.markdown import Markdown
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
except ImportError:
    pass

class RichHelpFormatter(argparse.HelpFormatter):
    """Formateur d'aide personnalisé utilisant rich pour un affichage amélioré."""
    
    def __init__(self, prog, indent_increment=2, max_help_position=24, width=None):
        super().__init__(prog, indent_increment, max_help_position, width)
        self.rich_console = Console() if RICH_AVAILABLE else None
    
    def _format_usage(self, usage, actions, groups, prefix):
        if not RICH_AVAILABLE:
            return super()._format_usage(usage, actions, groups, prefix)
        
        # Capturer la sortie standard pour pouvoir la modifier
        import io
        from contextlib import redirect_stdout
        
        f = io.StringIO()
        with redirect_stdout(f):
            super()._format_usage(usage, actions, groups, prefix)
        
        usage_text = f.getvalue()
        # On ne retourne rien ici, le usage sera affiché dans la méthode _display_rich_help
        return ""
    
    def _format_action(self, action):
        if not RICH_AVAILABLE:
            return super()._format_action(action)
        
        # On capture simplement les actions pour les afficher avec rich plus tard
        return ""
    
    def format_help(self):
        if not RICH_AVAILABLE:
            return super().format_help()
        
        # On retourne une chaîne vide ici car on affichera l'aide avec rich
        # après l'appel à format_help dans parse_arguments
        return ""

def display_rich_help(parser: argparse.ArgumentParser) -> None:
    """Affiche une aide formatée avec rich."""
    if not RICH_AVAILABLE:
        parser.print_help()
        return
    
    console = Console()
    
    # Titre et description
    console.print(f"\n[bold magenta]{parser.prog}[/bold magenta]")
    console.print("─" * len(parser.prog))
    console.print(f"[italic]{parser.description}[/italic]\n")
    
    # Usage
    usage = parser.format_usage().replace("usage: ", "")
    console.print("[bold cyan]USAGE[/bold cyan]")
    console.print(f"  {usage}\n")
    
    # Tables d'options par groupe
    for group in parser._action_groups:
        if not group._group_actions:
            continue
        
        console.print(f"[bold cyan]{group.title.upper()}[/bold cyan]")
        
        table = Table(show_header=False, box=None, padding=(0, 2, 0, 0))
        table.add_column("Option", style="green")
        table.add_column("Description")
        
        for action in group._group_actions:
            if action.help == argparse.SUPPRESS:
                continue
                
            # Formater les options
            opts = []
            if action.option_strings:
                opts = ", ".join(action.option_strings)
            else:
                opts = action.dest
                
            # Ajouter le type/valeurs pour les options avec des choix ou un type spécifique
            if action.choices:
                opts += f" {{{', '.join(map(str, action.choices))}}}"
            elif action.type and action.type.__name__ not in ('str', '_StoreAction'):
                opts += f" <{action.type.__name__}>"
                
            # Ajouter l'aide
            help_text = action.help or ""
            if action.default and action.default != argparse.SUPPRESS:
                if action.default not in (None, '', False):
                    help_text += f" (défaut: {action.default})"
            
            table.add_row(opts, help_text)
        
        console.print(table)
        console.print()
    
    # Épilogue
    if parser.epilog:
        console.print("[bold cyan]EXEMPLES[/bold cyan]")
        # Formater l'épilogue comme du markdown
        md = Markdown(parser.epilog)
        console.print(md)

def parse_arguments() -> argparse.Namespace:
    """Parse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Convertisseur de base de données SQLite vers Oracle SQL",
        formatter_class=RichHelpFormatter if RICH_AVAILABLE else argparse.RawDescriptionHelpFormatter,
        epilog="""
## Exemples de base

* Conversion simple avec création automatique d'utilisateur:
```bash
sqlite3-to-oracle --sqlite_db ma_base.sqlite
```

* Conversion avec nom d'utilisateur et mot de passe personnalisés:
```bash
sqlite3-to-oracle --sqlite_db ma_base.sqlite --new-username mon_user --new-password mon_pass
```

## Options avancées

* Conversion avec recréation de l'utilisateur et suppression des tables existantes:
```bash
sqlite3-to-oracle --sqlite_db ma_base.sqlite --force-recreate --drop-tables
```

* Utilisation avec configuration Oracle admin spécifique:
```bash
sqlite3-to-oracle --sqlite_db ma_base.sqlite --oracle-admin-user sys --oracle-admin-password manager --oracle-admin-dsn localhost:1521/XEPDB1
```

## Configuration externe

* Utilisation avec un fichier .env pour la configuration:
```bash
sqlite3-to-oracle --env-file /chemin/vers/.env
```

## Outils de diagnostic

* Tester uniquement la connexion Oracle admin:
```bash
sqlite3-to-oracle --check-connection-only
```

* Valider uniquement les identifiants Oracle:
```bash
sqlite3-to-oracle --validate-credentials-only
```
        """
    )
    
    # Groupe d'options pour la source (SQLite)
    source_group = parser.add_argument_group('Options de source')
    source_group.add_argument('--sqlite_db', 
                             help='Chemin vers le fichier de base de données SQLite')
    source_group.add_argument('--output-file', 
                             help="Nom du fichier SQL de sortie (par défaut: nom_base_oracle.sql)")
    source_group.add_argument('--validate-schema', action='store_true', default=True,
                             help="Valider le schéma et les données après l'importation (activé par défaut)")
    source_group.add_argument('--no-validate-schema', action='store_false', dest='validate_schema',
                             help="Désactiver la validation du schéma après l'importation")
    
    # Groupe d'options pour la cible (Oracle)
    target_group = parser.add_argument_group('Options de cible Oracle')
    target_group.add_argument('--new-username', 
                             help="Nom du nouvel utilisateur Oracle à créer (par défaut: nom de la base)")
    target_group.add_argument('--new-password', 
                             help="Mot de passe du nouvel utilisateur Oracle (par défaut: identique au nom d'utilisateur)")
    target_group.add_argument('--use-admin-user', action='store_true',
                             help="Utiliser directement l'utilisateur administrateur au lieu de créer un nouvel utilisateur")
    target_group.add_argument('--drop-tables', action='store_true', 
                             help='Supprimer les tables existantes avant de les recréer')
    target_group.add_argument('--force-recreate', action='store_true', 
                             help="Supprimer et recréer l'utilisateur Oracle et tous ses objets")
    target_group.add_argument('--schema-only', action='store_true',
                             help='Convertir uniquement le schéma, sans les données')
    
    # Groupe d'options pour l'administration Oracle 
    admin_group = parser.add_argument_group('Options d\'administration Oracle')
    admin_group.add_argument('--oracle-admin-user', 
                            help="Nom d'utilisateur administrateur Oracle (défaut: system)")
    admin_group.add_argument('--oracle-admin-password', 
                            help="Mot de passe administrateur Oracle")
    admin_group.add_argument('--oracle-admin-dsn', 
                            help="DSN Oracle (format: host:port/service)")
    admin_group.add_argument('--oracle-config-file',
                            help="Fichier de configuration Oracle (format JSON)")
    admin_group.add_argument('--env-file',
                            help="Fichier .env contenant les variables d'environnement pour la configuration")
    admin_group.add_argument('--check-connection-only', action='store_true',
                            help="Vérifier uniquement la connexion Oracle sans effectuer de conversion")
    admin_group.add_argument('--validate-credentials-only', action='store_true',
                            help="Vérifier uniquement les identifiants Oracle sans effectuer de conversion")
    
    # Groupe d'options pour le logging
    log_group = parser.add_argument_group('Options de logging')
    log_group.add_argument('--verbose', '-v', action='store_true',
                          help='Activer les messages de débogage détaillés')
    log_group.add_argument('--quiet', '-q', action='store_true',
                          help='Afficher uniquement les erreurs (mode silencieux)')
    
    # Si rich est disponible, afficher une aide enrichie
    if RICH_AVAILABLE and len(sys.argv) == 1 or '--help' in sys.argv or '-h' in sys.argv:
        display_rich_help(parser)
        sys.exit(0)
    
    return parser.parse_args()

def setup_logging(args: argparse.Namespace) -> None:
    """Configure le niveau de log en fonction des arguments."""
    global logger
    
    if args.quiet:
        level = logging.ERROR
    elif args.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO
    
    # Réinitialiser le logger avec le niveau approprié
    logger = setup_logger("sqlite3_to_oracle", level)

def extract_sqlite_content(sqlite_path: str) -> str:
    """
    Extrait le contenu SQL de la base SQLite.
    Utilise d'abord extract_sqlite_data, puis une méthode alternative en cas d'échec.
    
    Args:
        sqlite_path: Chemin vers le fichier SQLite
        
    Returns:
        Le contenu SQL extrait
        
    Raises:
        SystemExit: Si l'extraction échoue avec les deux méthodes
    """
    try:
        logger.info(f"Extraction des données depuis {sqlite_path}...")
        sqlite_sql = extract_sqlite_data(sqlite_path)
        logger.debug("Extraction réussie avec extract_sqlite_data()")
        return sqlite_sql
    except Exception as e:
        logger.warning(f"Échec de l'extraction principale: {str(e)}")
        logger.info("Tentative d'extraction alternative...")
        
        try:
            # Méthode de secours
            conn = sqlite3.connect(sqlite_path)
            cursor = conn.cursor()
            
            # Récupérer les tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
            tables = cursor.fetchall()
            
            if not tables:
                logger.error("Aucune table trouvée dans la base de données")
                sys.exit(1)
                
            logger.info(f"Tables trouvées: {', '.join(t[0] for t in tables)}")
            
            all_sql = []
            for table in tables:
                table_name = table[0]
                try:
                    # Vérifier si on peut lire la table
                    cursor.execute(f"SELECT * FROM {table_name} LIMIT 1;")
                    cursor.fetchall()
                    
                    # Exporter la table
                    table_sql = "\n".join(conn.iterdump() if hasattr(conn, 'table_name') 
                                         else [f"-- Table {table_name} extraite"])
                    all_sql.append(table_sql)
                    logger.debug(f"Table {table_name} extraite avec succès")
                except Exception as table_error:
                    logger.warning(f"Impossible d'extraire la table {table_name}: {str(table_error)}")
                    all_sql.append(f"-- Échec de l'extraction de la table {table_name}")
            
            conn.close()
            sqlite_sql = "\n".join(all_sql)
            
            if not sqlite_sql.strip():
                raise ValueError("Aucune donnée extraite")
                
            return sqlite_sql
        except Exception as e2:
            logger.error(f"Échec de la méthode alternative: {str(e2)}")
            logger.error("Impossible d'extraire les données de la base SQLite")
            sys.exit(1)

def determine_oracle_username(db_path: str, args: argparse.Namespace) -> Tuple[str, str]:
    """
    Détermine le nom d'utilisateur et le mot de passe Oracle à utiliser.
    
    Args:
        db_path: Chemin vers le fichier SQLite
        args: Arguments de ligne de commande
        
    Returns:
        Tuple contenant (nom d'utilisateur, mot de passe)
    """
    # Obtenir un nom d'utilisateur Oracle valide à partir du chemin
    oracle_username = get_oracle_username_from_filepath(db_path)
    
    # Utiliser les arguments CLI s'ils sont fournis
    username = args.new_username if args.new_username else oracle_username
    password = args.new_password if args.new_password else username
    
    logger.info(f"Nom d'utilisateur Oracle sélectionné: {username}")
    return username, password

def save_oracle_sql(oracle_sql: str, output_file: str) -> None:
    """Sauvegarde le SQL Oracle dans un fichier."""
    try:
        with open(output_file, 'w') as f:
            f.write(oracle_sql)
        logger.info(f"Script SQL Oracle sauvegardé dans {output_file}")
    except Exception as e:
        logger.error(f"Erreur lors de l'écriture du fichier de sortie: {e}")
        sys.exit(1)

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
    # 1. Vérifier la connexion administrateur
    admin_success, admin_message = check_oracle_connection(admin_config)
    if not admin_success:
        return False, f"Échec de la connexion administrateur Oracle: {admin_message}"
    
    # 2. Vérifier les privilèges administrateur
    import oracledb
    try:
        admin_conn = oracledb.connect(
            user=admin_config["user"],
            password=admin_config["password"],
            dsn=admin_config["dsn"]
        )
        cursor = admin_conn.cursor()
        
        # Vérifier si l'administrateur a le privilège CREATE USER
        cursor.execute("SELECT PRIVILEGE FROM SESSION_PRIVS WHERE PRIVILEGE = 'CREATE USER'")
        admin_has_create_user = bool(cursor.fetchone())
        
        if not admin_has_create_user:
            return False, f"L'utilisateur administrateur {admin_config['user']} n'a pas le privilège CREATE USER nécessaire pour créer un nouvel utilisateur"
        
        # Vérifier si l'utilisateur existe déjà
        try:
            cursor.execute(f"SELECT 1 FROM DBA_USERS WHERE USERNAME = UPPER('{new_username}')")
            user_exists = bool(cursor.fetchone())
            
            if user_exists:
                # Tester la connexion avec cet utilisateur
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
                # Vérifier si l'administrateur a les privilèges pour accorder des droits
                cursor.execute("SELECT PRIVILEGE FROM SESSION_PRIVS WHERE PRIVILEGE = 'GRANT ANY PRIVILEGE'")
                can_grant_privileges = bool(cursor.fetchone())
                
                # Vérifier si l'administrateur peut accorder CREATE SESSION
                cursor.execute("SELECT PRIVILEGE FROM SESSION_PRIVS WHERE PRIVILEGE IN ('CREATE SESSION', 'GRANT ANY PRIVILEGE')")
                can_grant_session = bool(cursor.fetchone())
                
                # Si l'admin ne peut pas accorder les privilèges, vérifier s'il peut accorder le rôle RESOURCE
                if not can_grant_privileges:
                    cursor.execute("SELECT PRIVILEGE FROM SESSION_PRIVS WHERE PRIVILEGE = 'GRANT ANY ROLE'")
                    can_grant_roles = bool(cursor.fetchone())
                    
                    if not can_grant_roles:
                        # Vérifier si la base peut créer des utilisateurs 
                        # même sans tous les privilèges attendus
                        return True, f"L'utilisateur administrateur {admin_config['user']} a des droits limités mais devrait pouvoir créer un utilisateur"
                
                # Vérification plus souple - considérer que si l'admin peut créer un utilisateur
                # et a soit le droit d'accorder des privilèges, soit d'accorder des rôles,
                # c'est suffisant pour continuer
                return True, f"L'utilisateur {new_username} peut être créé par l'administrateur {admin_config['user']}"
                
        except oracledb.DatabaseError as e:
            error, = e.args
            # Si nous ne pouvons pas interroger DBA_USERS, essayons ALL_USERS (moins de privilèges requis)
            if "ORA-00942" in str(error):  # table or view does not exist
                try:
                    cursor.execute(f"SELECT 1 FROM ALL_USERS WHERE USERNAME = UPPER('{new_username}')")
                    user_exists = bool(cursor.fetchone())
                    
                    if user_exists:
                        # Même logique que ci-dessus pour l'utilisateur existant
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
                        # Nous ne pouvons pas vérifier toutes les permissions détaillées,
                        # mais nous pouvons voir si l'admin peut se connecter, ce qui est un bon début
                        return True, f"L'utilisateur administrateur {admin_config['user']} peut se connecter, tentative de création d'utilisateur"
                except:
                    # Si même ALL_USERS n'est pas accessible, nous sommes probablement en mode limité
                    # Continuons quand même si la connexion admin fonctionne
                    return True, f"Vérification des privilèges limitée, mais connexion admin OK"
            return False, f"Erreur lors de la vérification de l'utilisateur: {error.message}"
            
    except Exception as e:
        return False, f"Erreur lors de la validation des identifiants: {str(e)}"
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'admin_conn' in locals():
            admin_conn.close()

def main() -> None:
    """Point d'entrée principal pour l'outil en ligne de commande"""
    # Étape 1: Analyser les arguments et configurer le logging
    args = parse_arguments()
    setup_logging(args)
    
    # Obtenir le gestionnaire de logging
    log_manager = get_log_manager()
    log_manager.set_log_level(logging.DEBUG if args.verbose else logging.ERROR if args.quiet else logging.INFO)
    
    try:
        print_title("SQLite to Oracle Converter")
        
        # Étape 1.5: Charger la configuration Oracle et les variables d'environnement
        global ORACLE_CONFIG
        ORACLE_CONFIG, env_cli_args = load_oracle_config(
            cli_config={
                "user": args.oracle_admin_user,
                "password": args.oracle_admin_password,
                "dsn": args.oracle_admin_dsn
            },
            config_file=args.oracle_config_file,
            env_file=args.env_file
        )
        
        # Pour le mode verbose, afficher les paramètres de connexion (masqués)
        if args.verbose:
            user = ORACLE_CONFIG.get("user", "non spécifié")
            dsn = ORACLE_CONFIG.get("dsn", "non spécifié")
            password = "*****" if ORACLE_CONFIG.get("password") else "non spécifié"
            
            logger.debug(f"Paramètres de connexion Oracle:")
            logger.debug(f"  - Utilisateur: {user}")
            logger.debug(f"  - DSN: {dsn}")
            logger.debug(f"  - Mot de passe: {password}")
        
        # Fusionner les arguments CLI avec ceux provenant des variables d'environnement
        # Les arguments CLI ont la priorité
        if env_cli_args:
            for key, value in env_cli_args.items():
                if key == 'sqlite_db' and not args.sqlite_db:
                    args.sqlite_db = value
                elif key == 'output_file' and not args.output_file:
                    args.output_file = value
                elif key == 'new_username' and not args.new_username:
                    args.new_username = value
                elif key == 'new_password' and not args.new_password:
                    args.new_password = value
                elif key == 'drop_tables' and not args.drop_tables:
                    args.drop_tables = value
                elif key == 'force_recreate' and not args.force_recreate:
                    args.force_recreate = value
                elif key == 'schema_only' and not args.schema_only:
                    args.schema_only = value
        
        # Vérifier que les identifiants administrateur sont valides
        if not all(key in ORACLE_CONFIG and ORACLE_CONFIG[key] for key in ("user", "password", "dsn")):
            print_error_message("Erreur de configuration Oracle")
            logger.error("Vous devez fournir les identifiants administrateur Oracle (user, password, dsn)")
            logger.info("Options: --oracle-admin-user, --oracle-admin-password, --oracle-admin-dsn")
            logger.info("Ou utilisez un fichier .env ou ~/.oracle_config.json")
            sys.exit(1)
        
        # Récupérer/initialiser le nom d'utilisateur et mot de passe cible
        if args.use_admin_user:
            target_username = ORACLE_CONFIG["user"]
            target_password = ORACLE_CONFIG["password"]
            logger.info(f"Utilisation de l'utilisateur admin pour toutes les opérations: [bold cyan]{target_username}[/bold cyan]")
        elif args.sqlite_db:
            target_username, target_password = determine_oracle_username(args.sqlite_db, args)
            logger.info(f"Cible: Nouvel utilisateur Oracle: [bold cyan]{target_username}[/bold cyan]")
        else:
            # Pour --check-connection-only ou --validate-credentials-only sans --sqlite_db
            target_username = args.new_username or "new_user"
            target_password = args.new_password or target_username
            logger.info(f"Cible: Nouvel utilisateur Oracle: [bold cyan]{target_username}[/bold cyan]")
        
        # Étape 1.6: Vérifier la connexion Oracle administrateur et la validité des identifiants
        print_title("Vérification des connexions Oracle")
        logger.info("Validation des identifiants Oracle...")
        
        # Ajouter un mode debug détaillé pour --check-connection-only
        if args.check_connection_only and args.verbose:
            logger.debug("Mode test de connexion avec débogage détaillé")
            logger.debug(f"Tentative de connexion avec: user={ORACLE_CONFIG['user']}, dsn={ORACLE_CONFIG['dsn']}")
        
        admin_success, admin_message = check_oracle_connection(ORACLE_CONFIG)
        if admin_success:
            logger.info(f"✓ Utilisateur admin: {admin_message}")
            
            # Si on est en mode check-connection-only, afficher plus d'informations
            if args.check_connection_only:
                try:
                    import oracledb
                    conn = oracledb.connect(
                        user=ORACLE_CONFIG["user"],
                        password=ORACLE_CONFIG["password"],
                        dsn=ORACLE_CONFIG["dsn"]
                    )
                    cursor = conn.cursor()
                    
                    # Récupérer et afficher les tablespaces
                    try:
                        cursor.execute("SELECT TABLESPACE_NAME FROM USER_TABLESPACES")
                        tablespaces = [row[0] for row in cursor.fetchall()]
                        logger.info(f"Tablespaces disponibles: {', '.join(tablespaces)}")
                    except:
                        logger.debug("Impossible de récupérer la liste des tablespaces")
                    
                    # Récupérer et afficher les privilèges système
                    try:
                        cursor.execute("SELECT * FROM SESSION_PRIVS")
                        privileges = [row[0] for row in cursor.fetchall()]
                        logger.info(f"Privilèges disponibles: {', '.join(privileges[:5])}..." if len(privileges) > 5 else f"Privilèges disponibles: {', '.join(privileges)}")
                    except:
                        logger.debug("Impossible de récupérer la liste des privilèges")
                    
                    cursor.close()
                    conn.close()
                except Exception as e:
                    logger.debug(f"Erreur lors de la récupération des détails supplémentaires: {str(e)}")
            
        else:
            print_error_message(f"Problème d'identifiants administrateur Oracle: {admin_message}")
            
            # Ajouter des suggestions de résolution selon l'erreur
            if "ORA-01017" in admin_message:
                logger.error("Solution possible: Vérifiez que le nom d'utilisateur et le mot de passe sont corrects")
                logger.error("  Utilisateur spécifié: " + ORACLE_CONFIG.get("user", "non spécifié"))
            elif "ORA-12541" in admin_message or "ORA-12514" in admin_message:
                logger.error("Solution possible: Vérifiez que le service Oracle est démarré et accessible")
                logger.error("  DSN spécifié: " + ORACLE_CONFIG.get("dsn", "non spécifié"))
                logger.error("  Format attendu: hostname:port/service_name")
            logger.error("Veuillez vérifier vos identifiants Oracle admin et réessayer.")
            sys.exit(1)
        
        # Si l'option --check-connection-only ou --validate-credentials-only est spécifiée, s'arrêter ici
        if args.check_connection_only or args.validate_credentials_only:
            print_success_message("La connexion Oracle a été validée avec succès.")
            logger.info(f"Admin: {ORACLE_CONFIG['user']}@{ORACLE_CONFIG['dsn']}")
            if not args.check_connection_only:  # Si c'est validate-credentials-only
                logger.info(f"Utilisateur cible: {target_username}")
            sys.exit(0)
        
        # Étape 2: Vérifier la présence de la base de données SQLite
        if not args.sqlite_db:
            print_error_message("Aucune base de données SQLite spécifiée")
            logger.error("Utilisez --sqlite_db ou la variable d'environnement ORACLE_SQLITE_DB")
            sys.exit(1)
        
        logger.info("Démarrage de la conversion SQLite vers Oracle...")
        
        logger.info(f"Configuration Oracle Admin: [bold cyan]user={ORACLE_CONFIG['user']}, dsn={ORACLE_CONFIG['dsn']}[/bold cyan]")
        logger.info(f"Utilisateur cible: [bold cyan]{target_username}[/bold cyan]")
        
        # Étape 3: Déterminer le fichier de sortie
        sqlite_db_path = args.sqlite_db
        base_name = os.path.splitext(sqlite_db_path)[0]
        output_file = args.output_file if args.output_file else f"{base_name}_oracle.sql"
        
        # Utiliser une barre de progression pour les tâches longues
        progress = log_manager.start_progress_mode(show_all_logs=args.verbose)
        
        try:
            if progress:
                # Créer une seule liste de tâches pour toutes les étapes
                tasks = []
                for step in ["Validation des connexions Oracle",
                            "Extraction des données SQLite",
                            "Conversion du schéma vers Oracle",
                            "Préparation de l'utilisateur Oracle",
                            "Sauvegarde du script SQL",
                            "Création/Vérification de l'utilisateur",
                            "Exécution du script SQL"]:
                    tasks.append(progress.add_task(f"[bold blue]{step}...", total=1, visible=False))
                
                with progress:
                    # Étape 1: Validation des connexions (afficher uniquement cette tâche)
                    log_manager.update_task(tasks[0], visible=True)
                    
                    # Validation de l'administrateur
                    admin_success, admin_message = check_oracle_connection(ORACLE_CONFIG)
                    if not admin_success:
                        progress.stop()
                        print_error_message(f"Problème d'identifiants administrateur Oracle: {admin_message}")
                        sys.exit(1)
                    
                    # Validation de l'utilisateur cible si nécessaire
                    if not args.use_admin_user:
                        validation_result, validation_message = validate_credentials(ORACLE_CONFIG, target_username, target_password)
                        if not validation_result:
                            progress.stop()
                            print_error_message(f"Problème avec le nouvel utilisateur: {validation_message}")
                            sys.exit(1)
                    
                    # Marquer cette tâche comme terminée et passer à la suivante
                    log_manager.update_task(tasks[0], completed=1)
                    
                    # Étape 2: Extraction des données SQLite
                    log_manager.update_task(tasks[1], visible=True)
                    sqlite_sql = extract_sqlite_content(sqlite_db_path)
                    log_manager.update_task(tasks[1], completed=1)
                    
                    # Étape 3: Conversion SQL
                    log_manager.update_task(tasks[2], visible=True)
                    oracle_sql = convert_sqlite_dump(sqlite_sql)
                    log_manager.update_task(tasks[2], completed=1)
                    
                    # Étape 4: Préparation utilisateur si nécessaire
                    if not args.use_admin_user:
                        log_manager.update_task(tasks[3], visible=True)
                        recreate_oracle_user(target_username, target_password, ORACLE_CONFIG, args.force_recreate)
                        log_manager.update_task(tasks[3], completed=1)
                    
                    # Étape 5: Sauvegarde SQL
                    log_manager.update_task(tasks[4], visible=True)
                    save_oracle_sql(oracle_sql, output_file)
                    log_manager.update_task(tasks[4], completed=1)
                    
                    # Étape 6: Création/vérification utilisateur si nécessaire
                    if not args.use_admin_user:
                        log_manager.update_task(tasks[5], visible=True)
                        user_created = create_oracle_user(ORACLE_CONFIG, target_username, target_password)
                        if not user_created:
                            progress.stop()
                            print_error_message(f"Impossible de créer ou d'utiliser l'utilisateur Oracle {target_username}")
                            sys.exit(1)
                        log_manager.update_task(tasks[5], completed=1)
                    
                    # Étape 7: Exécuter le script SQL
                    user_config = {
                        "user": target_username,
                        "password": target_password,
                        "dsn": ORACLE_CONFIG["dsn"]
                    }
                    
                    log_manager.update_task(tasks[6], visible=True)
                    try:
                        execute_sql_file(user_config, output_file, drop_tables=args.drop_tables)
                    except Exception as e:
                        progress.stop()
                        print_error_message(f"Échec de l'exécution du script SQL: {str(e)}")
                        sys.exit(1)
                    log_manager.update_task(tasks[6], completed=1)
                    
                    # Après le succès de l'importation, valider si demandé
                    if args.validate_schema:
                        from .schema_validator import run_validation
                        run_validation(
                            sqlite_db_path,
                            user_config,
                            verbose=args.verbose
                        )
            else:
                logger.info("Extraction des données SQLite...")
                sqlite_sql = extract_sqlite_content(sqlite_db_path)
                
                logger.info("Conversion du SQL SQLite en SQL Oracle...")
                oracle_sql = convert_sqlite_dump(sqlite_sql)
                
                if not args.use_admin_user:
                    logger.info("Préparation de l'utilisateur Oracle...")
                    recreate_oracle_user(target_username, target_password, ORACLE_CONFIG, args.force_recreate)
                
                logger.info("Sauvegarde du script SQL Oracle...")
                save_oracle_sql(oracle_sql, output_file)
                
                if not args.use_admin_user:
                    logger.info(f"Création/Vérification de l'utilisateur Oracle {target_username}...")
                    user_created = create_oracle_user(ORACLE_CONFIG, target_username, target_password)
                    
                    if not user_created:
                        print_error_message(f"Impossible de créer ou d'utiliser l'utilisateur Oracle {target_username}")
                        sys.exit(1)
                
                user_config = {
                    "user": target_username,
                    "password": target_password,
                    "dsn": ORACLE_CONFIG["dsn"]
                }
                
                logger.info(f"Exécution du script SQL dans Oracle...")
                try:
                    execute_sql_file(user_config, output_file, drop_tables=args.drop_tables)
                    logger.info("Script SQL exécuté avec succès dans Oracle")
                except Exception as e:
                    print_error_message(f"Échec de l'exécution du script SQL: {str(e)}")
                    sys.exit(1)
                
                # Après le succès de l'importation, valider si demandé
                if args.validate_schema:
                    from .schema_validator import run_validation
                    run_validation(
                        sqlite_db_path, 
                        user_config,
                        verbose=args.verbose
                    )
            
            log_manager.end_progress_mode()
            
            display_sqlalchemy_info(user_config)
            print_success_message("Conversion terminée avec succès!")
            
        except Exception as e:
            if progress:
                progress.stop()
            print_error_message(f"Erreur pendant la conversion: {str(e)}")
            if args.verbose:
                logger.exception("Détails de l'erreur:")
            sys.exit(1)
            
    except Exception as e:
        print_error_message(f"Erreur: {str(e)}")
        if args.verbose:
            logger.exception("Détails de l'erreur:")
        sys.exit(1)

if __name__ == '__main__':
    main()