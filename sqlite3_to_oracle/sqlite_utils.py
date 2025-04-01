"""
Utilitaires pour les opérations liées à SQLite.

Ce module fournit des fonctions utilitaires pour travailler avec des bases de données SQLite,
comme l'extraction de contenu SQL et la gestion des structures de données.
"""
import sys
from typing import Dict, List, Tuple, Any, Optional

from . import logger
from .converter import extract_sqlite_data
from .rich_logging import print_error_message, print_exception

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
        logger.info(f"Extraction du contenu de la base SQLite: {sqlite_path}")
        sql_content = extract_sqlite_data(sqlite_path)
        logger.debug(f"Extraction réussie: {len(sql_content)} caractères")
        return sql_content
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction de la base SQLite: {str(e)}")
        print_error_message(f"Échec de l'extraction de {sqlite_path}")
        print_exception(e, show_traceback=True)
        sys.exit(1)
