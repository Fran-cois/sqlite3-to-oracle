"""
MariaDB/SQLite to Oracle Converter

Un outil pour convertir des bases de données SQLite vers Oracle SQL.
"""

import logging
import sys
from .config import DEFAULT_ORACLE_CONFIG

__version__ = "0.1.0"

# Configuration par défaut pour Oracle - sera remplacée à l'exécution
ORACLE_CONFIG = DEFAULT_ORACLE_CONFIG

# Configuration du logger
logger = logging.getLogger('mariadb_to_oracle')
logger.setLevel(logging.INFO)

# Handler pour la console
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# Format de log
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Ajouter le handler au logger
logger.addHandler(console_handler)

from .converter import (
    convert_sqlite_dump,
    extract_sqlite_data,
)

from .oracle_utils import (
    create_oracle_user,
    execute_sql_file,
    get_sqlalchemy_uri,
)
