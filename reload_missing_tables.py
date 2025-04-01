#!/usr/bin/env python3
"""
Script pour recharger les tables manquantes ou problématiques identifiées dans une validation.

Note: Cette fonctionnalité est également disponible via la commande principale:
    sqlite3-to-oracle --sqlite_db <db_path> --retry [--use-varchar]
"""

import argparse
import os
import sys
import logging
from sqlite3_to_oracle import logger, ORACLE_CONFIG
from sqlite3_to_oracle.config import load_oracle_config
from sqlite3_to_oracle.data_loader import load_performance_table, reload_missing_tables

def parse_args():
    parser = argparse.ArgumentParser(description="Recharger les tables manquantes après une validation")
    parser.add_argument("--sqlite-path", required=True, help="Chemin vers le fichier SQLite")
    parser.add_argument("--table-name", help="Nom spécifique de la table à recharger (optionnel)")
    parser.add_argument("--report-file", help="Fichier contenant le rapport de validation (optionnel)")
    parser.add_argument("--oracle-config-file", help="Fichier de configuration Oracle (format JSON)")
    parser.add_argument("--env-file", help="Fichier .env contenant les variables d'environnement")
    parser.add_argument("--oracle-user", help="Nom d'utilisateur Oracle")
    parser.add_argument("--oracle-password", help="Mot de passe Oracle")
    parser.add_argument("--oracle-dsn", help="DSN Oracle (format: host:port/service)")
    parser.add_argument("--use-varchar", action="store_true", help="Utiliser VARCHAR2 pour les colonnes décimales problématiques")
    parser.add_argument("--verbose", "-v", action="store_true", help="Afficher les messages de débogage")
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Afficher le message d'information sur l'option --retry
    logging.info("Note: Cette fonctionnalité est également disponible via la commande principale:")
    logging.info("    sqlite3-to-oracle --sqlite_db <db_path> --retry [--use-varchar]")
    
    # Configurer le niveau de log
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level)
    
    # Charger la configuration Oracle
    global ORACLE_CONFIG
    ORACLE_CONFIG, _ = load_oracle_config(
        cli_config={
            "user": args.oracle_user,
            "password": args.oracle_password,
            "dsn": args.oracle_dsn
        },
        config_file=args.oracle_config_file,
        env_file=args.env_file
    )
    
    if not all(key in ORACLE_CONFIG and ORACLE_CONFIG[key] for key in ("user", "password", "dsn")):
        logging.error("Configuration Oracle incomplète")
        logging.info("Utilisez --oracle-user, --oracle-password, et --oracle-dsn, ou un fichier de configuration")
        sys.exit(1)
    
    logging.info(f"Utilisation de Oracle: {ORACLE_CONFIG['user']}@{ORACLE_CONFIG['dsn']}")
    
    # Vérifier le fichier SQLite
    if not os.path.exists(args.sqlite_path):
        logging.error(f"Le fichier SQLite {args.sqlite_path} n'existe pas")
        sys.exit(1)
    
    # Si une table spécifique est fournie, la recharger directement
    if args.table_name:
        logging.info(f"Rechargement de la table spécifique: {args.table_name}")
        
        if "ON_TIME" in args.table_name.upper():
            # Utiliser VARCHAR2 pour les colonnes décimales si demandé
            success = load_performance_table(ORACLE_CONFIG, args.sqlite_path, args.table_name, use_varchar_for_decimals=args.use_varchar)
            
            if args.use_varchar:
                logging.info("Utilisation de VARCHAR2 pour stocker les valeurs numériques problématiques")
        else:
            from sqlite3_to_oracle.data_loader import load_table_alternative
            success = load_table_alternative(ORACLE_CONFIG, args.sqlite_path, args.table_name)
        
        if success:
            logging.info(f"Rechargement réussi pour {args.table_name}")
        else:
            logging.error(f"Échec du rechargement pour {args.table_name}")
        
        sys.exit(0 if success else 1)
    
    # Si un rapport est fourni, l'analyser pour identifier les tables manquantes
    if args.report_file:
        if not os.path.exists(args.report_file):
            logging.error(f"Le fichier de rapport {args.report_file} n'existe pas")
            sys.exit(1)
        
        with open(args.report_file, 'r') as f:
            report_content = f.read()
        
        results = reload_missing_tables(report_content, ORACLE_CONFIG, args.sqlite_path)
        
        if results:
            successful = sum(1 for success in results.values() if success)
            logging.info(f"Rechargement terminé: {successful}/{len(results)} tables rechargées avec succès")
            
            for table, success in results.items():
                if success:
                    logging.info(f"✓ {table}: Rechargement réussi")
                else:
                    logging.error(f"✗ {table}: Échec du rechargement")
            
            sys.exit(0 if successful == len(results) else 1)
        else:
            logging.info("Aucune table n'a été rechargée")
            sys.exit(0)
    
    # Si ni table spécifique ni rapport n'est fourni
    logging.error("Vous devez spécifier soit une table spécifique (--table-name), soit un fichier de rapport (--report-file)")
    sys.exit(1)

if __name__ == "__main__":
    main()
