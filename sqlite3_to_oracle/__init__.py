"""
SQLite to Oracle Converter

Un outil pour convertir des bases de données SQLite vers Oracle SQL.
"""

import logging
import sys
from .config import DEFAULT_ORACLE_CONFIG
from .rich_logging import setup_logger

__version__ = "0.1.0"

# Configuration par défaut pour Oracle - sera remplacée à l'exécution
ORACLE_CONFIG = DEFAULT_ORACLE_CONFIG

# Configuration du logger moderne
logger = setup_logger("sqlite3_to_oracle", logging.INFO)

# Import des modules après la configuration du logger
from .converter import (
    convert_sqlite_dump,
    extract_sqlite_data,
)

from .oracle_utils import (
    create_oracle_user,
    execute_sql_file,
    get_sqlalchemy_uri,
)

from .schema_validator import (
    run_validation,
)