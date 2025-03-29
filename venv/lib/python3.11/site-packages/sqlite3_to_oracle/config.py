"""
Module de gestion de la configuration Oracle.
"""

import os
import json
from typing import Dict, Optional, Tuple
from pathlib import Path

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

def get_config_from_env() -> Tuple[Dict[str, str], Dict[str, any]]:
    """
    Charge la configuration Oracle depuis les variables d'environnement.
    
    Les variables cherchées sont:
    - ORACLE_ADMIN_USER: Nom d'utilisateur admin Oracle (défaut: system)
    - ORACLE_ADMIN_PASSWORD: Mot de passe admin Oracle
    - ORACLE_ADMIN_DSN: DSN Oracle (format: host:port/service)
    - ORACLE_NEW_USERNAME: Nom du nouvel utilisateur à créer
    - ORACLE_NEW_PASSWORD: Mot de passe du nouvel utilisateur
    - ORACLE_SQLITE_DB: Chemin vers la base de données SQLite
    - ORACLE_OUTPUT_FILE: Fichier SQL de sortie
    
    Returns:
        Tuple contenant:
        - Dictionnaire de configuration Oracle admin
        - Dictionnaire d'options CLI issues des variables d'environnement
    """
    config = dict(DEFAULT_ORACLE_CONFIG)
    env_cli_config = {}
    
    # Configuration admin Oracle
    if os.environ.get('ORACLE_ADMIN_USER'):
        config["user"] = os.environ.get('ORACLE_ADMIN_USER')
    
    if os.environ.get('ORACLE_ADMIN_PASSWORD'):
        config["password"] = os.environ.get('ORACLE_ADMIN_PASSWORD')
    
    if os.environ.get('ORACLE_ADMIN_DSN'):
        config["dsn"] = os.environ.get('ORACLE_ADMIN_DSN')
    
    # Autres variables d'environnement pour les options CLI
    if os.environ.get('ORACLE_NEW_USERNAME'):
        env_cli_config["new_username"] = os.environ.get('ORACLE_NEW_USERNAME')
    
    if os.environ.get('ORACLE_NEW_PASSWORD'):
        env_cli_config["new_password"] = os.environ.get('ORACLE_NEW_PASSWORD')
    
    if os.environ.get('ORACLE_SQLITE_DB'):
        env_cli_config["sqlite_db"] = os.environ.get('ORACLE_SQLITE_DB')
    
    if os.environ.get('ORACLE_OUTPUT_FILE'):
        env_cli_config["output_file"] = os.environ.get('ORACLE_OUTPUT_FILE')
    
    if os.environ.get('ORACLE_DROP_TABLES'):
        env_cli_config["drop_tables"] = os.environ.get('ORACLE_DROP_TABLES').lower() in ('true', 'yes', '1')
    
    if os.environ.get('ORACLE_FORCE_RECREATE'):
        env_cli_config["force_recreate"] = os.environ.get('ORACLE_FORCE_RECREATE').lower() in ('true', 'yes', '1')
    
    if os.environ.get('ORACLE_SCHEMA_ONLY'):
        env_cli_config["schema_only"] = os.environ.get('ORACLE_SCHEMA_ONLY').lower() in ('true', 'yes', '1')
    
    if os.environ.get('ORACLE_USE_ADMIN_USER'):
        env_cli_config["use_admin_user"] = os.environ.get('ORACLE_USE_ADMIN_USER').lower() in ('true', 'yes', '1')
    
    return config, env_cli_config

def get_config_from_file(config_file: str = None) -> Optional[Dict[str, str]]:
    """
    Charge la configuration Oracle depuis un fichier JSON.
    
    Args:
        config_file: Chemin vers le fichier de configuration (défaut: ~/.oracle_config.json)
    
    Returns:
        Dictionnaire de configuration Oracle ou None si fichier non trouvé/invalide
    """
    if not config_file:
        home_dir = Path.home()
        config_file = str(home_dir / '.oracle_config.json')
    
    try:
        with open(config_file, 'r') as f:
            config_data = json.load(f)
            
            # Vérifier la présence des champs requis
            if all(k in config_data for k in ("user", "password", "dsn")):
                return {
                    "user": config_data["user"],
                    "password": config_data["password"],
                    "dsn": config_data["dsn"]
                }
            else:
                return None
    except (FileNotFoundError, json.JSONDecodeError, PermissionError):
        return None

def load_oracle_config(cli_config: Dict[str, str] = None, config_file: str = None, env_file: str = None) -> Tuple[Dict[str, str], Dict[str, any]]:
    """
    Charge la configuration Oracle en respectant l'ordre de priorité suivant:
    1. Paramètres de ligne de commande
    2. Variables d'environnement (incluant le fichier .env)
    3. Fichier de configuration JSON
    4. Valeurs par défaut
    
    Args:
        cli_config: Paramètres de configuration passés en ligne de commande
        config_file: Chemin vers le fichier de configuration JSON
        env_file: Chemin vers le fichier .env
    
    Returns:
        Tuple contenant:
        - Dictionnaire de configuration Oracle final
        - Dictionnaire d'options CLI issues des variables d'environnement
    """
    # Charger les variables d'environnement à partir du fichier .env
    if env_file:
        load_dotenv_file(env_file)
    
    # Configuration par défaut
    config = dict(DEFAULT_ORACLE_CONFIG)
    
    # Charger depuis le fichier de configuration JSON
    file_config = get_config_from_file(config_file)
    if file_config:
        config.update(file_config)
    
    # Charger depuis les variables d'environnement (priorité supérieure)
    env_config, env_cli_config = get_config_from_env()
    config.update(env_config)
    
    # Charger depuis les paramètres CLI (priorité maximale)
    if cli_config:
        # Ne mettre à jour que les valeurs non None
        for key, value in cli_config.items():
            if value is not None:
                config[key] = value
    
    return config, env_cli_config