"""
Module de gestion de la configuration Oracle.
"""

import os
import json
import logging
from typing import Dict, Optional, Tuple, Any
from pathlib import Path
from . import ORACLE_CONFIG, logger

# Configuration par défaut pour Oracle
DEFAULT_ORACLE_CONFIG = {
    "user": "system",
    "password": "YourPassword",
    "dsn": "localhost:1521/free"
}

def load_dotenv_file(env_file: str = None) -> bool:
    """
    Charge les variables d'environnement à partir d'un fichier .env
    
    Args:
        env_file: Chemin vers le fichier .env à charger
    
    Returns:
        bool: True si le chargement a réussi, False sinon
    """
    try:
        from dotenv import load_dotenv
        
        if env_file and os.path.isfile(env_file):
            # Charger le fichier .env spécifié
            load_dotenv(env_file)
            return True
        else:
            # Essayer de charger le fichier .env par défaut
            default_env = os.path.join(os.getcwd(), '.env')
            if os.path.isfile(default_env):
                load_dotenv(default_env)
                return True
        
        return False
    except ImportError:
        return False

def load_oracle_config(
    cli_config: Dict[str, Optional[str]] = None,
    config_file: Optional[str] = None, 
    env_file: Optional[str] = None
) -> Tuple[Dict[str, str], Dict[str, Any]]:
    """
    Charge la configuration Oracle à partir de plusieurs sources avec priorité:
    1. Arguments CLI (priorité la plus élevée)
    2. Fichier .env spécifié
    3. Variables d'environnement
    4. Fichier ~/.oracle_config.json
    5. Valeurs par défaut (priorité la plus basse)
    
    Args:
        cli_config: Configuration fournie par les arguments en ligne de commande
        config_file: Chemin vers un fichier de configuration JSON
        env_file: Chemin vers un fichier .env
        
    Returns:
        Tuple contenant (config_oracle, variables_env_cli)
    """
    # Commencer avec la configuration par défaut
    oracle_config = ORACLE_CONFIG.copy()
    
    # Variables d'environnement à extraire pour la CLI
    env_cli_vars = {}
    
    # Charger depuis fichier .env si spécifié
    if env_file and os.path.exists(env_file):
        try:
            # Import conditionnel pour éviter une dépendance stricte
            try:
                from dotenv import load_dotenv
                # Charger les variables d'environnement depuis le fichier .env
                load_dotenv(env_file)
                logger.info(f"Variables d'environnement chargées depuis {env_file}")
            except ImportError:
                logger.warning("Le module python-dotenv n'est pas installé, impossible de charger le fichier .env")
                logger.info("Installez-le avec: pip install python-dotenv")
        except Exception as e:
            logger.warning(f"Erreur lors du chargement du fichier .env: {str(e)}")
    
    # Vérifier les variables d'environnement
    env_mapping = {
        "ORACLE_ADMIN_USER": "user",
        "ORACLE_ADMIN_PASSWORD": "password",
        "ORACLE_ADMIN_DSN": "dsn",
    }
    
    for env_var, config_key in env_mapping.items():
        if os.environ.get(env_var):
            oracle_config[config_key] = os.environ.get(env_var)
    
    # Essayer de charger depuis ~/.oracle_config.json si existant
    home_dir = os.path.expanduser("~")
    default_config_file = os.path.join(home_dir, ".oracle_config.json")
    
    if os.path.exists(default_config_file):
        try:
            with open(default_config_file, 'r') as f:
                user_config = json.load(f)
                # Mettre à jour uniquement les clés existantes
                for key in oracle_config:
                    if key in user_config and user_config[key]:
                        oracle_config[key] = user_config[key]
            logger.debug(f"Configuration Oracle chargée depuis {default_config_file}")
        except Exception as e:
            logger.debug(f"Erreur lors du chargement de {default_config_file}: {str(e)}")
    
    # Charger depuis le fichier de configuration JSON spécifié
    if config_file and os.path.exists(config_file):
        try:
            with open(config_file, 'r') as f:
                file_config = json.load(f)
                # Mettre à jour uniquement les clés existantes
                for key in oracle_config:
                    if key in file_config and file_config[key]:
                        oracle_config[key] = file_config[key]
            logger.debug(f"Configuration Oracle chargée depuis {config_file}")
        except Exception as e:
            logger.warning(f"Erreur lors du chargement de {config_file}: {str(e)}")
    
    # Extraire les variables d'environnement utiles pour la CLI
    cli_env_vars = [
        ("ORACLE_SQLITE_DB", "sqlite_db"),
        ("ORACLE_OUTPUT_FILE", "output_file"),
        ("ORACLE_NEW_USERNAME", "new_username"),
        ("ORACLE_NEW_PASSWORD", "new_password"),
        ("ORACLE_DROP_TABLES", "drop_tables"),
        ("ORACLE_FORCE_RECREATE", "force_recreate"),
        ("ORACLE_SCHEMA_ONLY", "schema_only"),
        ("ORACLE_BATCH", "batch"),
        ("ORACLE_SQLITE_DIR", "sqlite_dir"),
        ("ORACLE_FILE_PATTERN", "file_pattern"),
        ("ORACLE_URI_OUTPUT_FILE", "uri_output_file"),
        ("ORACLE_CONTINUE_ON_ERROR", "continue_on_error"),
        ("ORACLE_USE_VARCHAR", "use_varchar"),
        ("ORACLE_ONLY_FK_KEYS", "only_fk_keys"),  # Ajout de la nouvelle option
    ]
    
    for env_var, cli_var in cli_env_vars:
        env_value = os.environ.get(env_var)
        if env_value:
            # Convertir les valeurs booléennes
            if env_value.lower() in ('true', 'yes', '1'):
                env_cli_vars[cli_var] = True
            elif env_value.lower() in ('false', 'no', '0'):
                env_cli_vars[cli_var] = False
            else:
                env_cli_vars[cli_var] = env_value
    
    # Appliquer la configuration CLI (priorité la plus élevée)
    if cli_config:
        for key, value in cli_config.items():
            if value is not None:  # Ne pas écraser avec None
                oracle_config[key] = value
    
    # Valider la configuration finale
    missing_keys = [key for key in ["user", "password", "dsn"] if not oracle_config.get(key)]
    if missing_keys:
        logger.warning(f"Configuration Oracle incomplète: {', '.join(missing_keys)} manquant")
    
    return oracle_config, env_cli_vars

def save_oracle_config(config: Dict[str, str], config_file: str = None) -> bool:
    """
    Sauvegarde la configuration Oracle dans un fichier JSON.
    
    Args:
        config: Configuration Oracle à sauvegarder
        config_file: Chemin du fichier de configuration (défaut: ~/.oracle_config.json)
        
    Returns:
        True si la sauvegarde a réussi, False sinon
    """
    if not config_file:
        home_dir = os.path.expanduser("~")
        config_file = os.path.join(home_dir, ".oracle_config.json")
    
    try:
        # Vérifier si le répertoire existe
        config_dir = os.path.dirname(config_file)
        if config_dir and not os.path.exists(config_dir):
            try:
                os.makedirs(config_dir)
            except Exception as e:
                logger.warning(f"Impossible de créer le répertoire {config_dir}: {str(e)}")
                return False
        
        # Sauvegarder la configuration
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
        
        # Ajuster les permissions pour protéger le mot de passe
        try:
            import stat
            os.chmod(config_file, stat.S_IRUSR | stat.S_IWUSR)  # Permissions 600 (lecture/écriture uniquement par l'utilisateur)
        except Exception as e:
            logger.warning(f"Impossible d'ajuster les permissions du fichier de configuration: {str(e)}")
        
        logger.info(f"Configuration Oracle sauvegardée dans {config_file}")
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde de la configuration: {str(e)}")
        return False

def get_connection_string(config: Dict[str, str]) -> str:
    """
    Génère une chaîne de connexion à partir de la configuration Oracle.
    
    Args:
        config: Configuration Oracle
        
    Returns:
        Chaîne de connexion au format "user/password@dsn"
    """
    user = config.get("user", "")
    password = config.get("password", "")
    dsn = config.get("dsn", "")
    
    # Masquer le mot de passe pour le logging
    if password:
        masked_password = "*" * len(password)
        logger.debug(f"Chaîne de connexion générée: {user}/{masked_password}@{dsn}")
    
    return f"{user}/{password}@{dsn}"