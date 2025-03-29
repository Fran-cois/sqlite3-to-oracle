"""
Module de gestion de la configuration Oracle.
"""

import os
import json
from typing import Dict, Optional
from pathlib import Path

# Configuration par défaut pour Oracle
DEFAULT_ORACLE_CONFIG = {
    "user": "system",
    "password": "YourPassword",
    "dsn": "localhost:1521/free"
}

def get_config_from_env() -> Dict[str, str]:
    """
    Charge la configuration Oracle depuis les variables d'environnement.
    
    Les variables cherchées sont:
    - ORACLE_ADMIN_USER: Nom d'utilisateur admin Oracle (défaut: system)
    - ORACLE_ADMIN_PASSWORD: Mot de passe admin Oracle
    - ORACLE_ADMIN_DSN: DSN Oracle (format: host:port/service)
    
    Returns:
        Dictionnaire de configuration Oracle
    """
    config = dict(DEFAULT_ORACLE_CONFIG)
    
    if os.environ.get('ORACLE_ADMIN_USER'):
        config["user"] = os.environ.get('ORACLE_ADMIN_USER')
    
    if os.environ.get('ORACLE_ADMIN_PASSWORD'):
        config["password"] = os.environ.get('ORACLE_ADMIN_PASSWORD')
    
    if os.environ.get('ORACLE_ADMIN_DSN'):
        config["dsn"] = os.environ.get('ORACLE_ADMIN_DSN')
    
    return config

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

def load_oracle_config(cli_config: Dict[str, str] = None, config_file: str = None) -> Dict[str, str]:
    """
    Charge la configuration Oracle en respectant l'ordre de priorité suivant:
    1. Paramètres de ligne de commande
    2. Variables d'environnement
    3. Fichier de configuration
    4. Valeurs par défaut
    
    Args:
        cli_config: Paramètres de configuration passés en ligne de commande
        config_file: Chemin vers le fichier de configuration
    
    Returns:
        Dictionnaire de configuration Oracle final
    """
    # Configuration par défaut
    config = dict(DEFAULT_ORACLE_CONFIG)
    
    # Charger depuis le fichier de configuration
    file_config = get_config_from_file(config_file)
    if file_config:
        config.update(file_config)
    
    # Charger depuis les variables d'environnement (priorité supérieure)
    env_config = get_config_from_env()
    config.update(env_config)
    
    # Charger depuis les paramètres CLI (priorité maximale)
    if cli_config:
        # Ne mettre à jour que les valeurs non None
        for key, value in cli_config.items():
            if value is not None:
                config[key] = value
    
    return config
