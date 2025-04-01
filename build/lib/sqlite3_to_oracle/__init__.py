"""
Module pour convertir des bases de données SQLite en Oracle.
"""

import os
import logging
import json
from typing import Dict, Any

# Configuration initiale du logger
logger = logging.getLogger("sqlite3_to_oracle")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
logger.addHandler(handler)

# Configuration Oracle par défaut
ORACLE_CONFIG = {
    "user": os.environ.get("ORACLE_ADMIN_USER", "system"),
    "password": os.environ.get("ORACLE_ADMIN_PASSWORD", ""),
    "dsn": os.environ.get("ORACLE_ADMIN_DSN", "localhost:1521/XEPDB1")
}

# Essayer de charger la configuration depuis ~/.oracle_config.json
home_dir = os.path.expanduser("~")
config_file = os.path.join(home_dir, ".oracle_config.json")

if os.path.exists(config_file):
    try:
        with open(config_file, 'r') as f:
            user_config = json.load(f)
            # Mettre à jour uniquement les clés existantes
            for key in ORACLE_CONFIG:
                if key in user_config and user_config[key]:
                    ORACLE_CONFIG[key] = user_config[key]
        logger.debug(f"Configuration Oracle chargée depuis {config_file}")
    except Exception as e:
        logger.debug(f"Erreur lors du chargement de la configuration: {e}")

# Exposer les modules nouvellement créés
from .data_loader import (
    extract_table_structure,
    load_table_alternative,
    load_failing_tables,
    reload_missing_tables
)

from .lookup_loader import (
    create_simplified_lookup_table,
    parse_and_load_lookup_data
)

from .performance_loader import load_performance_table

from .table_utils import (
    process_large_table,
    diagnose_and_fix_ora_00922,
    sanitize_create_table_statement
)

# Version du package
__version__ = "1.0.0"