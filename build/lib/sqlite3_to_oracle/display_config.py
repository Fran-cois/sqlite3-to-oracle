"""
Configuration des options d'affichage et de logging.
Ce module permet de personnaliser l'apparence et le comportement des logs.
"""

from typing import Dict, List, Any

# Configuration des tableaux de résumé  
TABLE_CONFIG = {
    "show_borders": True,
    "padding": (0, 1, 0, 1),
    "header_style": "bold cyan",
    "row_styles": ["", "dim"]
}

# Niveaux de détail pour le logging
LOG_LEVELS = {
    "QUIET": {
        "show_progress": True,
        "show_errors": True,
        "show_warnings": False,
        "show_summaries": True
    },
    "NORMAL": {
        "show_progress": True,
        "show_errors": True,
        "show_warnings": True,
        "show_summaries": True,
        "show_info": True,
        "collapse_similar_messages": True
    },
    "VERBOSE": {
        "show_progress": True,
        "show_errors": True,
        "show_warnings": True,
        "show_summaries": True,
        "show_info": True,
        "show_debug": True,
        "collapse_similar_messages": False
    }
}

# Étapes du workflow et leurs descriptions pour la barre de progression
WORKFLOW_STEPS = [
    {
        "id": "connect",
        "description": "Validation des connexions Oracle",
        "success_message": "Connexions Oracle validées",
        "error_message": "Échec de validation des connexions"
    },
    {
        "id": "extract",
        "description": "Extraction des données SQLite",
        "success_message": "Données SQLite extraites",
        "error_message": "Échec d'extraction des données"
    },
    {
        "id": "convert",
        "description": "Conversion du schéma vers Oracle",
        "success_message": "Schéma converti avec succès",
        "error_message": "Échec de conversion du schéma"
    },
    {
        "id": "user",
        "description": "Préparation de l'utilisateur Oracle",
        "success_message": "Utilisateur Oracle préparé",
        "error_message": "Échec de préparation de l'utilisateur"
    },
    {
        "id": "execute",
        "description": "Exécution du script SQL",
        "success_message": "Script SQL exécuté avec succès",
        "error_message": "Échec d'exécution du script SQL"
    },
    {
        "id": "finalize",
        "description": "Finalisation de la base de données",
        "success_message": "Base de données finalisée",
        "error_message": "Erreur lors de la finalisation"
    }
]

# Messages d'erreur et suggestions de résolution
ERROR_SUGGESTIONS = {
    "ORA-01017": [
        "Vérifiez que le nom d'utilisateur est correct",
        "Vérifiez que le mot de passe est correct",
        "L'utilisateur pourrait être verrouillé"
    ],
    "ORA-12541": [
        "Vérifiez que le serveur Oracle est démarré",
        "Vérifiez que le listener Oracle est actif",
        "Vérifiez le nom d'hôte et le port"
    ],
    "ORA-12514": [
        "Vérifiez le nom du service Oracle",
        "Le service pourrait ne pas être démarré",
        "Format DSN attendu: host:port/service"
    ],
    "ORA-42299": [
        "Cette erreur peut survenir lors de la suppression d'un utilisateur Oracle avec des sessions actives",
        "Essayez de terminer toutes les sessions Oracle de cet utilisateur",
        "Vérifiez les objets (tables, procédures, etc.) qui pourraient être utilisés par d'autres utilisateurs",
        "Documentation Oracle: https://docs.oracle.com/error-help/db/ora-42299/"
    ],
    "MISSING_TABLE_REFS": [
        "Les références à des tables non existantes sont normales lors de la première création du schéma",
        "Les contraintes de clé étrangère seront ajoutées après la création de toutes les tables",
        "Vérifiez que les noms des tables sont correctement orthographiés dans les contraintes FOREIGN KEY"
    ]
}

# Paramètres pour l'affichage des tables manquantes
MISSING_TABLES_CONFIG = {
    "group_by_reference": True,  # Grouper par table référencée plutôt que par table référençante
    "max_references_per_line": 3,  # Nombre maximum de références à afficher par ligne
    "show_in_verbose_only": True,  # Afficher uniquement en mode verbose
}

def get_display_config(mode: str = "NORMAL") -> Dict[str, Any]:
    """
    Retourne la configuration d'affichage selon le mode spécifié.
    
    Args:
        mode: QUIET, NORMAL ou VERBOSE
    
    Returns:
        Un dictionnaire de configuration
    """
    if mode not in LOG_LEVELS:
        mode = "NORMAL"
    
    return {
        "log_level": LOG_LEVELS[mode],
        "table_config": TABLE_CONFIG,
        "workflow_steps": WORKFLOW_STEPS,
        "error_suggestions": ERROR_SUGGESTIONS,
        "missing_tables_config": MISSING_TABLES_CONFIG
    }
