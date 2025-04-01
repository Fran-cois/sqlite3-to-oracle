"""
Exemple simple d'utilisation programmatique de sqlite3-to-oracle
"""

import os
import sys
import logging
from sqlite3_to_oracle import (
    load_oracle_config,
    extract_sqlite_data,
    convert_sqlite_dump,
    load_table_alternative
)

# Configurer le logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger('example')

def main():
    """Fonction principale de l'exemple."""
    # Vérifier les arguments
    if len(sys.argv) < 2:
        print("Usage: python exemple_simple.py <chemin_sqlite>")
        sys.exit(1)
    
    sqlite_path = sys.argv[1]
    if not os.path.exists(sqlite_path):
        logger.error(f"Le fichier {sqlite_path} n'existe pas")
        sys.exit(1)
    
    # 1. Charger la configuration Oracle
    oracle_config, _ = load_oracle_config()
    
    # Vérifier que la configuration est complète
    if not all(key in oracle_config and oracle_config[key] for key in ["user", "password", "dsn"]):
        logger.error("Configuration Oracle incomplète. Définissez les variables d'environnement suivantes:")
        logger.error("ORACLE_ADMIN_USER, ORACLE_ADMIN_PASSWORD, ORACLE_ADMIN_DSN")
        sys.exit(1)
    
    # 2. Extraire les données de la base SQLite
    logger.info(f"Extraction des données depuis {sqlite_path}")
    try:
        sqlite_sql = extract_sqlite_data(sqlite_path)
        logger.info(f"Extraction réussie: {len(sqlite_sql)} caractères")
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction: {e}")
        sys.exit(1)
    
    # 3. Convertir le SQL SQLite en SQL Oracle
    logger.info("Conversion du SQL vers le format Oracle")
    try:
        oracle_sql = convert_sqlite_dump(sqlite_sql)
        logger.info(f"Conversion réussie: {len(oracle_sql)} caractères")
        
        # 4. Sauvegarder le SQL Oracle dans un fichier
        output_path = os.path.splitext(sqlite_path)[0] + "_oracle.sql"
        with open(output_path, 'w') as f:
            f.write(oracle_sql)
        logger.info(f"SQL Oracle sauvegardé dans {output_path}")
    except Exception as e:
        logger.error(f"Erreur lors de la conversion: {e}")
        sys.exit(1)
    
    # 5. Option: Demander à l'utilisateur s'il veut également charger les données
    response = input("Voulez-vous également charger les données dans Oracle? (o/n): ")
    if response.lower() in ('o', 'oui', 'y', 'yes'):
        try:
            # Récupérer une table à charger (par exemple la première)
            import sqlite3
            conn = sqlite3.connect(sqlite_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
            sample_table = cursor.fetchone()[0]
            conn.close()
            
            # Charger cette table
            logger.info(f"Chargement de la table {sample_table}")
            result = load_table_alternative(oracle_config, sqlite_path, sample_table)
            
            if result:
                logger.info(f"Chargement réussi pour {sample_table}")
            else:
                logger.warning(f"Échec du chargement pour {sample_table}")
        except Exception as e:
            logger.error(f"Erreur lors du chargement: {e}")
    
    logger.info("Exemple terminé avec succès")

if __name__ == "__main__":
    main()
