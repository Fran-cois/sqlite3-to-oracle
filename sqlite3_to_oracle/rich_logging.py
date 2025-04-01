"""
Module pour gérer le logging avec une sortie enrichie (couleurs, formatage avancé).
Utilise le module 'rich' s'il est disponible, sinon revient à un logging standard.
"""

import sys
import logging
from typing import Optional, Any, List, Dict
import traceback

# Détection si rich est installé
try:
    from rich.console import Console
    from rich.logging import RichHandler
    from rich.progress import Progress, TaskID, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn
    from rich.style import Style
    from rich.panel import Panel
    from rich.theme import Theme
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

# Définir le thème de couleurs personnalisé pour rich
if RICH_AVAILABLE:
    CUSTOM_THEME = Theme({
        "info": "cyan",
        "warning": "yellow",
        "error": "bold red",
        "success": "bold green",
        "title": "bold blue",
        "highlight": "magenta",
        "debug": "dim white"
    })
    
    # Console principale avec notre thème
    console = Console(theme=CUSTOM_THEME)

# Style pour le logging ordinaire
LOG_FORMAT = '%(levelname)s - %(message)s'

class LogManager:
    """Gestionnaire de logs qui peut basculer entre les modes normal et progress."""
    
    def __init__(self, logger_name: str, level: int = logging.INFO):
        """
        Initialise le gestionnaire de logs.
        
        Args:
            logger_name: Nom du logger
            level: Niveau de logging initial
        """
        self.logger_name = logger_name
        self.level = level
        self.logger = logging.getLogger(logger_name)
        self.progress = None
        self.in_progress_mode = False
        self.setup_logger(level)
    
    def setup_logger(self, level: int):
        """
        Configure le logger avec le niveau spécifié.
        
        Args:
            level: Niveau de logging (DEBUG, INFO, etc.)
        """
        self.level = level
        self.logger.setLevel(level)
        
        # Supprimer les handlers existants
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # Configurer le handler selon que rich est disponible ou non
        if RICH_AVAILABLE:
            handler = RichHandler(console=console, show_path=False, enable_link_path=False)
            self.logger.addHandler(handler)
        else:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(LOG_FORMAT)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def set_log_level(self, level: int):
        """
        Change le niveau de logging.
        
        Args:
            level: Nouveau niveau de logging
        """
        self.setup_logger(level)
    
    def start_progress_mode(self, show_all_logs: bool = False) -> Optional[Progress]:
        """
        Bascule en mode progress pour afficher des barres de progression.
        
        Args:
            show_all_logs: Si True, continue d'afficher les logs en plus des barres de progression
            
        Returns:
            L'objet Progress si rich est disponible, sinon None
        """
        if not RICH_AVAILABLE:
            return None
        
        # Créer un objet Progress avec nos colonnes personnalisées
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console
        )
        
        self.in_progress_mode = True
        
        # Si on veut continuer à afficher les logs, reconfigurer le logger
        if show_all_logs:
            for handler in self.logger.handlers[:]:
                if isinstance(handler, RichHandler):
                    self.logger.removeHandler(handler)
            
            # Ajouter un nouveau handler qui utilise la même console que Progress
            new_handler = RichHandler(console=console, show_path=False, enable_link_path=False)
            self.logger.addHandler(new_handler)
        
        return self.progress
    
    def end_progress_mode(self):
        """Termine le mode progress et revient au mode logging normal."""
        self.in_progress_mode = False
        self.progress = None
        
        # Reconfigurer le logger standard
        self.setup_logger(self.level)
    
    def update_task(self, task_id: TaskID, completed: int = None, advance: int = None, visible: bool = None):
        """
        Met à jour une tâche dans la barre de progression.
        
        Args:
            task_id: ID de la tâche à mettre à jour
            completed: Définir l'avancement total
            advance: Incrémenter l'avancement
            visible: Définir la visibilité de la tâche
        """
        if self.in_progress_mode and self.progress:
            if completed is not None:
                self.progress.update(task_id, completed=completed)
            if advance is not None:
                self.progress.update(task_id, advance=advance)
            if visible is not None:
                self.progress.update(task_id, visible=visible)

# Singleton du gestionnaire de logs
_log_manager = None

def get_log_manager() -> LogManager:
    """
    Récupère ou crée le singleton LogManager.
    
    Returns:
        Instance LogManager
    """
    global _log_manager
    if _log_manager is None:
        _log_manager = LogManager("sqlite3_to_oracle")
    return _log_manager

def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Configure un logger avec une sortie enrichie si disponible.
    
    Args:
        name: Nom du logger
        level: Niveau de logging
    
    Returns:
        Logger configuré
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Supprimer les handlers existants
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Configurer avec rich si disponible
    if RICH_AVAILABLE:
        handler = RichHandler(console=console, show_path=False, enable_link_path=False)
        logger.addHandler(handler)
    else:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

def print_title(title: str):
    """
    Affiche un titre mis en forme.
    
    Args:
        title: Texte du titre
    """
    if RICH_AVAILABLE:
        console.print(f"\n[title]{title}[/title]")
        console.print("=" * len(title.strip()))
    else:
        print(f"\n{title}")
        print("=" * len(title.strip()))

def print_success_message(message: str):
    """
    Affiche un message de succès.
    
    Args:
        message: Message à afficher
    """
    if RICH_AVAILABLE:
        console.print(f"[success]✓ {message}[/success]")
    else:
        print(f"✓ {message}")

def print_error_message(message: str):
    """
    Affiche un message d'erreur.
    
    Args:
        message: Message d'erreur
    """
    if RICH_AVAILABLE:
        console.print(f"[error]✗ {message}[/error]")
    else:
        print(f"✗ {message}")

def print_warning_message(message: str):
    """
    Affiche un message d'avertissement.
    
    Args:
        message: Message d'avertissement
    """
    if RICH_AVAILABLE:
        console.print(f"[warning]⚠ {message}[/warning]")
    else:
        print(f"⚠ {message}")

def print_exception(exc: Exception, show_traceback: bool = False):
    """
    Affiche une exception de manière formatée.
    
    Args:
        exc: Exception à afficher
        show_traceback: Si True, affiche la trace complète
    """
    error_message = str(exc)
    
    if RICH_AVAILABLE:
        if show_traceback:
            console.print_exception(show_locals=False)
        else:
            console.print(f"[error]Erreur: {error_message}[/error]")
    else:
        print(f"Erreur: {error_message}")
        if show_traceback:
            traceback.print_exc()

def get_progress_bar() -> Optional[Any]:
    """
    Crée et retourne une barre de progression.
    
    Returns:
        Objet Progress si rich est disponible, sinon None
    """
    if not RICH_AVAILABLE:
        return None
    
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
        console=console
    )
