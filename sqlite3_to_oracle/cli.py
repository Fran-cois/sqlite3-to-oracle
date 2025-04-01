"""
Interface en ligne de commande pour le convertisseur SQLite vers Oracle.

Ce module fournit une interface utilisateur simple pour convertir une base de données
SQLite vers Oracle, en créant automatiquement l'utilisateur et en exécutant le script.
"""
import argparse
import sys
import os
import logging
import io
import glob
import time
from typing import Dict, Tuple, Optional, List, Any

from . import ORACLE_CONFIG, logger
from .config import load_oracle_config, save_oracle_config
from .converter import convert_sqlite_dump
from .oracle_utils import (
    create_oracle_user, 
    execute_sql_file, 
    display_sqlalchemy_info,
    recreate_oracle_user,
    get_oracle_username_from_filepath,
    check_oracle_connection,
    export_validation_report
)
from .schema_validator import run_validation
from .data_loader import reload_missing_tables
from .rich_logging import (
    setup_logger,
    print_title,
    print_success_message,
    print_error_message,
    print_warning_message,
    print_exception,
    get_progress_bar,
    RICH_AVAILABLE,
    get_log_manager
)
from .validation import (
    validate_single_schema, 
    validate_schema_with_output, 
    process_batch_validation,
    validate_credentials
)
from .sqlite_utils import extract_sqlite_content

if RICH_AVAILABLE:
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
        
        from contextlib import redirect_stdout
        
        f = io.StringIO()
        with redirect_stdout(f):
            super()._format_usage(usage, actions, groups, prefix)
        return ""

    def _format_action(self, action):
        if not RICH_AVAILABLE:
            return super()._format_action(action)
        
        return ""
    
    def format_help(self):
        if not RICH_AVAILABLE:
            return super().format_help()
        
        return ""

def display_rich_help(parser: argparse.ArgumentParser) -> None:
    """Affiche une aide formatée avec rich."""
    if not RICH_AVAILABLE:
        parser.print_help()
        return
    
    console = Console()
    
    console.print(f"\n[bold magenta]{parser.prog}[/bold magenta]")
    console.print("─" * len(parser.prog))
    console.print(f"[italic]{parser.description}[/italic]\n")
    
    usage = parser.format_usage().replace("usage: ", "")
    console.print("[bold cyan]USAGE[/bold cyan]")
    console.print(f"  {usage}\n")
    
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
                
            opts = []
            if action.option_strings:
                opts = ", ".join(action.option_strings)
            else:
                opts = action.dest
                
            if action.choices:
                opts += f" {{{', '.join(map(str, action.choices))}}}"
            elif action.type and action.type.__name__ not in ('str', '_StoreAction'):
                opts += f" <{action.type.__name__}>"
                
            help_text = action.help or ""
            if action.default and action.default != argparse.SUPPRESS:
                if action.default not in (None, '', False):
                    help_text += f" (défaut: {action.default})"
            
            table.add_row(opts, help_text)
        
        console.print(table)
        console.print()
    
    if parser.epilog:
        console.print("[bold cyan]EXEMPLES[/bold cyan]")
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
    
    source_group = parser.add_argument_group('Options de source')
    source_group.add_argument('--sqlite_db', 
                             help='Chemin vers le fichier de base de données SQLite')
    source_group.add_argument('--output-file', 
                             help="Nom du fichier SQL de sortie (par défaut: nom_base_oracle.sql)")
    source_group.add_argument('--validate-schema', action='store_true', default=True,
                             help="Valider le schéma et les données après l'importation (activé par défaut)")
    source_group.add_argument('--no-validate-schema', action='store_false', dest='validate_schema',
                             help="Désactiver la validation du schéma après l'importation")
    source_group.add_argument('--validate-schema-only', action='store_true',
                             help="Exécuter uniquement la validation du schéma sur une base déjà importée")
    source_group.add_argument('--retry', action='store_true',
                             help="Tenter de recharger les tables qui ont échoué lors d'une précédente importation")
    source_group.add_argument('--use-varchar', action='store_true', 
                             help="Utiliser VARCHAR2 pour les colonnes avec valeurs décimales problématiques")
    source_group.add_argument('--only-fk-keys', action='store_true', 
                              help="Ne conserver que les clés primaires et les colonnes utilisées dans les clés étrangères")
    source_group.add_argument('--disable-bitmap-indexes', action='store_true',
                              help="Désactiver la création automatique d'index bitmap")
    source_group.add_argument('--bitmap-ratio', type=float, default=0.1,
                              help="Ratio maximal pour les colonnes candidates aux index bitmap (défaut: 0.1)")
    source_group.add_argument('--exclude-tables-from-bitmap', 
                              help="Tables à exclure de la création d'index bitmap (séparées par des virgules)")
    
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
    
    log_group = parser.add_argument_group('Options de logging')
    log_group.add_argument('--verbose', '-v', action='store_true',
                          help='Activer les messages de débogage détaillés')
    log_group.add_argument('--quiet', '-q', action='store_true',
                          help='Afficher uniquement les erreurs (mode silencieux)')
    
    batch_group = parser.add_argument_group('Options de traitement par lots')
    batch_group.add_argument('--batch', action='store_true',
                            help="Activer le mode de traitement par lots pour importer plusieurs bases de données")
    batch_group.add_argument('--sqlite-dir',
                            help="Répertoire contenant les fichiers SQLite à importer en lot")
    batch_group.add_argument('--file-pattern', default="*.db",
                            help="Motif de fichiers à traiter (par défaut: *.sqlite)")
    batch_group.add_argument('--uri-output-file',
                            help="Fichier de sortie pour enregistrer les URIs SQLAlchemy générées")
    batch_group.add_argument('--continue-on-error', action='store_true',
                            help="Continuer le traitement par lots même en cas d'erreur sur une base")
    
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
    
    logger = setup_logger("sqlite3_to_oracle", level)

def determine_oracle_username(db_path: str, args: argparse.Namespace) -> Tuple[str, str]:
    """
    Détermine le nom d'utilisateur et le mot de passe Oracle à utiliser.
    
    Args:
        db_path: Chemin vers le fichier SQLite
        args: Arguments de ligne de commande
        
    Returns:
        Tuple contenant (nom d'utilisateur, mot de passe)
    """
    oracle_username = get_oracle_username_from_filepath(db_path)
    
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

def process_single_database(sqlite_db_path: str, args: argparse.Namespace, log_manager) -> Optional[Dict[str, str]]:
    try:
        if args.validate_schema:
            from .validation import validate_single_schema
            import io
            import sys
            
            user_config = {
                "user": args.new_username,
                "password": args.new_password,
                "dsn": ORACLE_CONFIG["dsn"]
            }
            
            success, report_file = validate_single_schema(
                sqlite_db_path,
                user_config,
                verbose=args.verbose
            )
            
            if report_file:
                logger.info(f"Rapport de validation exporté dans {report_file}")
        
        return {
            "user": args.new_username,
            "password": args.new_password,
            "dsn": ORACLE_CONFIG["dsn"],
            "source_db": sqlite_db_path
        }
    except Exception as e:
        logger.error(f"Erreur lors du traitement de la base: {str(e)}")
        return None

def main() -> None:
    import os
    
    args = parse_arguments()
    setup_logging(args)
    
    log_manager = get_log_manager()
    log_manager.set_log_level(logging.DEBUG if args.verbose else logging.ERROR if args.quiet else logging.INFO)
    
    try:
        print_title("SQLite to Oracle Converter")
        
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
        
        # Fusionner les arguments CLI avec ceux provenant des variables d'environnement
        # Les arguments CLI ont la priorité
        if env_cli_args:
            for key, value in env_cli_args.items():
                if args.verbose:
                    logger.debug(f"Traitement de la variable d'environnement {key}={value}")
                
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
                elif key == 'batch' and not args.batch:
                    args.batch = value
                    logger.debug(f"Mode batch activé depuis .env: {value}")
                elif key == 'sqlite_dir' and not args.sqlite_dir:
                    args.sqlite_dir = value
                    logger.debug(f"Répertoire SQLite défini via variable d'environnement: {value}")
                elif key == 'file_pattern' and not args.file_pattern:
                    args.file_pattern = value
                elif key == 'uri_output_file' and not args.uri_output_file:
                    args.uri_output_file = value
                elif key == 'continue_on_error' and not args.continue_on_error:
                    args.continue_on_error = value
        
        # Vérifier explicitement si le mode batch est activé depuis l'environnement
        batch_from_env = False
        if not args.batch:
            if os.environ.get('ORACLE_BATCH', '').lower() in ('true', 'yes', '1'):
                args.batch = True
                batch_from_env = True
                logger.debug("Mode batch activé directement depuis la variable d'environnement ORACLE_BATCH")
            elif env_cli_args and env_cli_args.get('batch') in (True, 'True', 'true', 'yes', '1', 1):
                args.batch = True
                batch_from_env = True
                logger.debug("Mode batch activé depuis le fichier d'environnement")
        
        # Vérifier que les identifiants administrateur sont valides
        if not all(key in ORACLE_CONFIG and ORACLE_CONFIG[key] for key in ("user", "password", "dsn")):
            print_error_message("Erreur de configuration Oracle")
            logger.error("Vous devez fournir les identifiants administrateur Oracle (user, password, dsn)")
            logger.info("Options: --oracle-admin-user, --oracle-admin-password, --oracle-admin-dsn")
            logger.info("Ou utilisez un fichier .env ou ~/.oracle_config.json")
            sys.exit(1)
        
        # Vérifier et afficher clairement le mode de fonctionnement
        if args.validate_schema_only and args.batch:
            logger.info("Mode batch de validation de schéma uniquement activé")
            
            if not args.sqlite_dir:
                # Vérifier si le répertoire SQLite est dans les variables d'environnement
                if 'ORACLE_SQLITE_DIR' in os.environ:
                    args.sqlite_dir = os.environ.get('ORACLE_SQLITE_DIR')
                    logger.info(f"Répertoire SQLite obtenu depuis ORACLE_SQLITE_DIR: {args.sqlite_dir}")
                elif env_cli_args and 'sqlite_dir' in env_cli_args:
                    args.sqlite_dir = env_cli_args['sqlite_dir']
                    logger.info(f"Répertoire SQLite obtenu depuis le fichier d'environnement: {args.sqlite_dir}")
                else:
                    print_error_message("Répertoire SQLite non spécifié pour le mode batch")
                    logger.error("Utilisez --sqlite-dir ou la variable d'environnement ORACLE_SQLITE_DIR")
                    sys.exit(1)
            
            if not os.path.isdir(args.sqlite_dir):
                print_error_message(f"Le répertoire {args.sqlite_dir} n'existe pas.")
                sys.exit(1)
            
            logger.info(f"Validation des bases SQLite dans le répertoire: {args.sqlite_dir}")
            successful_validations = process_batch_validation(
                oracle_config=ORACLE_CONFIG,
                sqlite_dir=args.sqlite_dir,
                file_pattern=args.file_pattern,
                use_admin_user=args.use_admin_user,
                new_username=args.new_username,
                new_password=args.new_password,
                continue_on_error=args.continue_on_error,
                verbose=args.verbose
            )
            
            if successful_validations:
                print_success_message(f"Validation par lots terminée avec {len(successful_validations)} bases validées avec succès")
                
                # Rechercher les rapports générés
                import glob
                
                # Rapport détaillé
                latest_report = None
                report_files = glob.glob(os.path.join(args.sqlite_dir, "validation_batch_report_*.md"))
                if report_files:
                    latest_report = max(report_files, key=os.path.getmtime)
                
                # Rapport global
                overall_report = os.path.join(args.sqlite_dir, "overall_report.md")
                has_overall_report = os.path.exists(overall_report)
                
                # Afficher les rapports si Rich est disponible
                if RICH_AVAILABLE and not args.quiet:
                    try:
                        from rich.console import Console
                        from rich.markdown import Markdown
                        
                        console = Console()
                        
                        # Afficher directement le rapport global s'il existe
                        if has_overall_report:
                            print(f"\nRapport global d'état:")
                            with open(overall_report, 'r', encoding='utf-8') as f:
                                md = Markdown(f.read())
                            console.print(md)
                            print(f"\nRapport global sauvegardé dans: {overall_report}")
                        
                        # Pour le rapport détaillé, indiquer simplement son emplacement
                        if latest_report:
                            print(f"\nRapport détaillé disponible: {latest_report}")
                            
                    except Exception as e:
                        logger.debug(f"Erreur lors de l'affichage des rapports: {str(e)}")
                
                # Afficher simplement les chemins des rapports si on est en mode silencieux ou sans Rich
                elif latest_report or has_overall_report:
                    if has_overall_report:
                        print(f"\nRapport global disponible: {overall_report}")
                    if latest_report:
                        print(f"Rapport détaillé disponible: {latest_report}")
            else:
                print_error_message("Aucune base n'a été validée avec succès")
                sys.exit(1)
            
            sys.exit(0)
        elif args.validate_schema_only:
            logger.info("Mode validation de schéma uniquement activé")
            
            # Si batch est détecté depuis les variables d'environnement mais pas spécifié explicitement
            if batch_from_env:
                logger.info("Mode batch détecté depuis les variables d'environnement")
                
                # Rediriger vers le mode batch
                if not args.sqlite_dir:
                    # Vérifier si le répertoire SQLite est dans les variables d'environnement
                    if 'ORACLE_SQLITE_DIR' in os.environ:
                        args.sqlite_dir = os.environ.get('ORACLE_SQLITE_DIR')
                        logger.info(f"Répertoire SQLite obtenu depuis ORACLE_SQLITE_DIR: {args.sqlite_dir}")
                    elif env_cli_args and 'sqlite_dir' in env_cli_args:
                        args.sqlite_dir = env_cli_args['sqlite_dir']
                        logger.info(f"Répertoire SQLite obtenu depuis le fichier d'environnement: {args.sqlite_dir}")
                    else:
                        print_error_message("Répertoire SQLite non spécifié pour le mode batch")
                        logger.error("Utilisez --sqlite-dir ou la variable d'environnement ORACLE_SQLITE_DIR")
                        sys.exit(1)
                
                if not os.path.isdir(args.sqlite_dir):
                    print_error_message(f"Le répertoire {args.sqlite_dir} n'existe pas.")
                    sys.exit(1)
                
                logger.info(f"Validation des bases SQLite dans le répertoire: {args.sqlite_dir}")
                successful_validations = process_batch_validation(
                    oracle_config=ORACLE_CONFIG,
                    sqlite_dir=args.sqlite_dir,
                    file_pattern=args.file_pattern,
                    use_admin_user=args.use_admin_user,
                    new_username=args.new_username,
                    new_password=args.new_password,
                    continue_on_error=args.continue_on_error,
                    verbose=args.verbose
                )
                
                if successful_validations:
                    print_success_message(f"Validation par lots terminée avec {len(successful_validations)} bases validées avec succès")
                    
                    # Rechercher les rapports générés
                    import glob
                    
                    # Rapport détaillé
                    latest_report = None
                    report_files = glob.glob(os.path.join(args.sqlite_dir, "validation_batch_report_*.md"))
                    if report_files:
                        latest_report = max(report_files, key=os.path.getmtime)
                    
                    # Rapport global
                    overall_report = os.path.join(args.sqlite_dir, "overall_report.md")
                    has_overall_report = os.path.exists(overall_report)
                    
                    # Afficher les rapports si Rich est disponible
                    if RICH_AVAILABLE and not args.quiet:
                        try:
                            from rich.console import Console
                            from rich.markdown import Markdown
                            
                            console = Console()
                            
                            # Afficher directement le rapport global s'il existe
                            if has_overall_report:
                                print(f"\nRapport global d'état:")
                                with open(overall_report, 'r', encoding='utf-8') as f:
                                    md = Markdown(f.read())
                                console.print(md)
                                print(f"\nRapport global sauvegardé dans: {overall_report}")
                            
                            # Pour le rapport détaillé, indiquer simplement son emplacement
                            if latest_report:
                                print(f"\nRapport détaillé disponible: {latest_report}")
                                
                        except Exception as e:
                            logger.debug(f"Erreur lors de l'affichage des rapports: {str(e)}")
                    
                    # Afficher simplement les chemins des rapports si on est en mode silencieux ou sans Rich
                    elif latest_report or has_overall_report:
                        if has_overall_report:
                            print(f"\nRapport global disponible: {overall_report}")
                        if latest_report:
                            print(f"Rapport détaillé disponible: {latest_report}")
                else:
                    print_error_message("Aucune base n'a été validée avec succès")
                    sys.exit(1)
                
                sys.exit(0)
            
            if not args.sqlite_db:
                print_error_message("Base de données SQLite source non spécifiée")
                logger.error("Utilisez --sqlite_db pour spécifier la base SQLite source à comparer")
                sys.exit(1)
            
            if args.use_admin_user:
                target_username = ORACLE_CONFIG["user"]
                target_password = ORACLE_CONFIG["password"]
            else:
                target_username, target_password = determine_oracle_username(args.sqlite_db, args)
            
            user_config = {
                "user": target_username,
                "password": target_password,
                "dsn": ORACLE_CONFIG["dsn"]
            }
            
            success = validate_schema_with_output(
                args.sqlite_db,
                user_config,
                verbose=args.verbose
            )
            
            sys.exit(0 if success else 1)
        
        if not args.sqlite_db:
            print_error_message("Aucune base de données SQLite spécifiée")
            logger.error("Utilisez --sqlite_db ou la variable d'environnement ORACLE_SQLITE_DB")
            logger.info("Ou activez le mode batch avec --batch et spécifiez un répertoire avec --sqlite-dir")
            sys.exit(1)
        
        sqlite_db_path = args.sqlite_db
        logger.info("Démarrage de la conversion SQLite vers Oracle...")
        logger.info(f"Configuration Oracle Admin: [bold cyan]user={ORACLE_CONFIG['user']}, dsn={ORACLE_CONFIG['dsn']}[/bold cyan]")
        
        user_config = process_single_database(sqlite_db_path, args, log_manager)
        
        if user_config:
            display_sqlalchemy_info(user_config)
            print_success_message("Conversion terminée avec succès!")
            
            if args.validate_schema:
                db_name = os.path.splitext(os.path.basename(sqlite_db_path))[0]
                report_file = f"{db_name}_validation_report.md"
                report_path = os.path.join(os.path.dirname(sqlite_db_path), report_file)
                if os.path.exists(report_path):
                    # Afficher directement le rapport si Rich est disponible
                    if RICH_AVAILABLE:
                        try:
                            from rich.console import Console
                            from rich.markdown import Markdown
                            
                            console = Console()
                            print(f"\nRapport de validation:")
                            with open(report_path, 'r', encoding='utf-8') as f:
                                md = Markdown(f.read())
                            console.print(md)
                            print(f"\nRapport sauvegardé dans: {report_path}")
                        except:
                            print(f"\nRapport de validation disponible: {report_path}")
                    else:
                        print(f"\nRapport de validation disponible: {report_path}")
        else:
            print_error_message("La conversion a échoué")
            sys.exit(1)
        
    except Exception as e:
        print_error_message(f"Erreur: {str(e)}")
        if args.verbose:
            logger.exception("Détails de l'erreur:")
        sys.exit(1)

if __name__ == '__main__':
    main()