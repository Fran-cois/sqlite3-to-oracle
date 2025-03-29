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
from typing import Dict, Tuple, Optional

from . import ORACLE_CONFIG, logger
from .config import load_oracle_config
from .converter import extract_sqlite_data, convert_sqlite_dump
from .oracle_utils import (
    create_oracle_user, 
    execute_sql_file, 
    display_sqlalchemy_info,
    recreate_oracle_user,
    get_oracle_username_from_filepath
)

def parse_arguments() -> argparse.Namespace:
    """Parse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Convertisseur de base de données SQLite vers Oracle SQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  # Conversion simple avec création automatique d'utilisateur
  mariadb-to-oracle --sqlite_db ma_base.sqlite
  
  # Conversion avec nom d'utilisateur et mot de passe personnalisés
  mariadb-to-oracle --sqlite_db ma_base.sqlite --new-username mon_user --new-password mon_pass
  
  # Conversion avec recréation de l'utilisateur et suppression des tables existantes
  mariadb-to-oracle --sqlite_db ma_base.sqlite --force-recreate --drop-tables
  
  # Utilisation avec configuration Oracle admin spécifique
  mariadb-to-oracle --sqlite_db ma_base.sqlite --oracle-admin-user sys --oracle-admin-password manager --oracle-admin-dsn localhost:1521/XEPDB1
        """
    )
    
    # Groupe d'options pour la source (SQLite)
    source_group = parser.add_argument_group('Options de source')
    source_group.add_argument('--sqlite_db', required=True, 
                             help='Chemin vers le fichier de base de données SQLite')
    source_group.add_argument('--output-file', 
                             help="Nom du fichier SQL de sortie (par défaut: nom_base_oracle.sql)")
    
    # Groupe d'options pour la cible (Oracle)
    target_group = parser.add_argument_group('Options de cible Oracle')
    target_group.add_argument('--new-username', 
                             help="Nom du nouvel utilisateur Oracle à créer (par défaut: nom de la base)")
    target_group.add_argument('--new-password', 
                             help="Mot de passe du nouvel utilisateur Oracle (par défaut: identique au nom d'utilisateur)")
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
    
    # Groupe d'options pour le logging
    log_group = parser.add_argument_group('Options de logging')
    log_group.add_argument('--verbose', '-v', action='store_true',
                          help='Activer les messages de débogage détaillés')
    log_group.add_argument('--quiet', '-q', action='store_true',
                          help='Afficher uniquement les erreurs (mode silencieux)')
    
    return parser.parse_args()

def setup_logging(args: argparse.Namespace) -> None:
    """Configure le niveau de log en fonction des arguments."""
    if args.quiet:
        logger.setLevel(logging.ERROR)
        for handler in logger.handlers:
            handler.setLevel(logging.ERROR)
    elif args.verbose:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
    else:
        # Niveau par défaut: INFO
        pass

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

def main() -> None:
    """Point d'entrée principal pour l'outil en ligne de commande"""
    # Étape 1: Analyser les arguments et configurer le logging
    args = parse_arguments()
    setup_logging(args)
    
    logger.info("Démarrage de la conversion SQLite vers Oracle...")
    
    # Étape 1.5: Charger la configuration Oracle
    global ORACLE_CONFIG
    ORACLE_CONFIG = load_oracle_config(
        cli_config={
            "user": args.oracle_admin_user,
            "password": args.oracle_admin_password,
            "dsn": args.oracle_admin_dsn
        },
        config_file=args.oracle_config_file
    )
    
    logger.info(f"Utilisation de la configuration Oracle Admin: user={ORACLE_CONFIG['user']}, dsn={ORACLE_CONFIG['dsn']}")
    
    # Étape 2: Déterminer le fichier de sortie
    sqlite_db_path = args.sqlite_db
    base_name = os.path.splitext(sqlite_db_path)[0]
    output_file = args.output_file if args.output_file else f"{base_name}_oracle.sql"
    
    # Étape 3: Extraire les données SQLite
    sqlite_sql = extract_sqlite_content(sqlite_db_path)
    
    # Étape 4: Convertir le SQL SQLite en SQL Oracle
    logger.info("Conversion du SQL SQLite en SQL Oracle...")
    oracle_sql = convert_sqlite_dump(sqlite_sql)
    
    # Étape 5: Déterminer les credentials Oracle
    new_username, new_password = determine_oracle_username(sqlite_db_path, args)
    
    # Étape 6: Recréer l'utilisateur Oracle si demandé
    recreate_oracle_user(new_username, new_password, ORACLE_CONFIG, args.force_recreate)
    
    # Étape 7: Sauvegarder le SQL Oracle dans un fichier
    save_oracle_sql(oracle_sql, output_file)
    
    # Étape 8: Créer l'utilisateur Oracle
    logger.info(f"Création/Vérification de l'utilisateur Oracle {new_username}...")
    create_oracle_user(ORACLE_CONFIG, new_username, new_password)
    
    # Étape 9: Exécuter le script SQL
    user_config = {
        "user": new_username,
        "password": new_password,
        "dsn": ORACLE_CONFIG["dsn"]
    }
    
    logger.info(f"Exécution du script SQL dans Oracle...")
    execute_sql_file(user_config, output_file, drop_tables=args.drop_tables)
    logger.info("Script SQL exécuté avec succès dans Oracle")
    
    # Étape 10: Afficher les informations de connexion
    display_sqlalchemy_info(user_config)
    
    logger.info("Conversion terminée avec succès!")

if __name__ == '__main__':
    main()
