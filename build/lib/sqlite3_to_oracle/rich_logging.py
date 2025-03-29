"""
Module pour configurer un logger moderne avec Rich.
"""

import logging
import sys
from typing import Optional, List

try:
    from rich.console import Console
    from rich.logging import RichHandler
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
    from rich.theme import Theme
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

# Définir une palette de couleurs personnalisée
custom_theme = {
    "info": "bold cyan",
    "warning": "bold yellow",
    "error": "bold red",
    "debug": "bold green",
    "success": "bold green",
    "title": "bold magenta",
    "sqlite": "bold blue",
    "oracle": "bold yellow",
}

class ColorFormatter(logging.Formatter):
    """Formateur de logs avec couleurs pour les terminaux qui supportent ANSI."""
    
    COLORS = {
        'DEBUG': '\033[92m',  # Vert
        'INFO': '\033[96m',   # Cyan
        'WARNING': '\033[93m', # Jaune
        'ERROR': '\033[91m',  # Rouge
        'CRITICAL': '\033[91m\033[1m',  # Rouge gras
        'RESET': '\033[0m',   # Reset
    }
    
    def format(self, record):
        log_message = super().format(record)
        if record.levelname in self.COLORS:
            log_message = f"{self.COLORS[record.levelname]}{log_message}{self.COLORS['RESET']}"
        return log_message

def setup_logger(name: str = "sqlite3_to_oracle", level: int = logging.INFO, use_rich: bool = True) -> logging.Logger:
    """
    Configure et retourne un logger moderne et coloré.
    
    Args:
        name: Nom du logger
        level: Niveau de logging
        use_rich: Utiliser Rich si disponible
    
    Returns:
        Un logger configuré
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Supprimer les handlers existants
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    if RICH_AVAILABLE and use_rich:
        # Configuration du thème Rich
        theme = Theme(custom_theme)
        console = Console(theme=theme)
        
        # Configuration du handler Rich
        rich_handler = RichHandler(
            console=console,
            show_path=False,
            omit_repeated_times=True,
            rich_tracebacks=True,
            tracebacks_extra_lines=1,
            tracebacks_show_locals=True
        )
        rich_handler.setLevel(level)
        
        # Format simple car Rich s'occupe de la date et du niveau
        formatter = logging.Formatter("%(message)s")
        rich_handler.setFormatter(formatter)
        
        logger.addHandler(rich_handler)
    else:
        # Fallback vers un handler console avec couleurs ANSI
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        
        # Format plus détaillé avec couleurs ANSI
        formatter = ColorFormatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
    
    return logger

def get_progress_bar() -> Optional[Progress]:
    """
    Crée et retourne une barre de progression Rich avec une mise en page améliorée.
    
    Returns:
        Une barre de progression ou None si Rich n'est pas disponible
    """
    if not RICH_AVAILABLE:
        return None
    
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(complete_style="green", finished_style="bold green"),
        TextColumn("[bold]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=Console(stderr=True),  # Utiliser stderr pour éviter de mélanger avec les logs
        expand=False,  # Ne pas remplir toute la largeur du terminal
        refresh_per_second=5,  # Réduire la fréquence de rafraîchissement pour éviter les duplications
        transient=True,  # Rendre la barre de progression transitoire (ne reste pas après complétion)
    )
    
    return progress

class ProgressHandler(logging.Handler):
    """Handler de logging qui met en pause la barre de progression pour afficher les logs."""
    
    def __init__(self, progress: Optional[Progress], level=logging.NOTSET, show_levels=('ERROR', 'CRITICAL')):
        super().__init__(level)
        self.progress = progress
        self.show_levels = show_levels
        
    def emit(self, record):
        if self.progress and record.levelname in self.show_levels:
            # Mettre en pause la barre de progression pour afficher les messages importants
            self.progress.stop()
            
            # Formater et afficher le message
            msg = self.format(record)
            print(f"\033[91m{msg}\033[0m" if record.levelname == 'ERROR' else msg)
            
            # Reprendre la barre de progression
            self.progress.start()

class LogManager:
    """Gestionnaire de logging qui ajuste dynamiquement la verbosité."""
    
    def __init__(self, name="sqlite3_to_oracle", level=logging.INFO):
        self.logger = setup_logger(name, level)
        self.progress = None
        self.log_level = level
        self.progress_handler = None
        self.original_handlers = list(self.logger.handlers)
        self._last_completed_tasks = set()  # Pour suivre les tâches complétées
        self._active_tasks = {}  # Dictionnaire des tâches actives
        
    def set_log_level(self, level):
        """Change le niveau de log."""
        self.log_level = level
        self.logger.setLevel(level)
        
    def start_progress_mode(self, show_all_logs=False):
        """
        Active le mode barre de progression.
        
        Args:
            show_all_logs: Si True, tous les messages seront affichés pendant la progression.
                          Si False, seuls les erreurs et avertissements seront affichés.
        """
        if RICH_AVAILABLE:
            # Réinitialiser l'état de suivi des tâches
            self._last_completed_tasks = set()
            self._active_tasks = {}
            
            # Créer une nouvelle barre de progression
            self.progress = get_progress_bar()
            
            # Configurer les logs pendant la progression
            if not show_all_logs:
                # Sauvegarder les handlers existants
                self.original_handlers = list(self.logger.handlers)
                
                # Supprimer les handlers existants
                for handler in self.logger.handlers[:]:
                    self.logger.removeHandler(handler)
                
                # Ajouter notre handler personnalisé
                show_levels = ('ERROR', 'CRITICAL') if self.log_level > logging.WARNING else ('ERROR', 'WARNING', 'CRITICAL')
                self.progress_handler = ProgressHandler(self.progress, level=self.log_level, show_levels=show_levels)
                self.logger.addHandler(self.progress_handler)
            
            return self.progress
        return None
    
    def update_task(self, task_id, description=None, total=None, completed=None, visible=True):
        """
        Met à jour une tâche dans la barre de progression.
        
        Args:
            task_id: ID de la tâche (de Progress.add_task)
            description: Nouvelle description
            total: Nouveau total
            completed: Nouvelle valeur de complétion
            visible: Si la tâche doit être visible
        """
        if not self.progress:
            return
            
        # Vérifier si cette tâche est déjà complétée
        if completed and completed >= (total or 1):
            # Si déjà marquée comme complétée, ne pas mettre à jour à nouveau
            if task_id in self._last_completed_tasks:
                return
            self._last_completed_tasks.add(task_id)
        
        # Mettre à jour la tâche
        update_kwargs = {}
        if description is not None:
            update_kwargs["description"] = description
        if total is not None:
            update_kwargs["total"] = total
        if completed is not None:
            update_kwargs["completed"] = completed
        if not visible:
            update_kwargs["visible"] = False
            
        # Stocker l'état de la tâche
        if task_id not in self._active_tasks:
            self._active_tasks[task_id] = {}
        for k, v in update_kwargs.items():
            self._active_tasks[task_id][k] = v
            
        # Mettre à jour la tâche
        if update_kwargs:
            self.progress.update(task_id, **update_kwargs)
        
    def end_progress_mode(self):
        """Désactive le mode barre de progression et restaure les handlers de log."""
        if self.progress_handler:
            self.logger.removeHandler(self.progress_handler)
            
            # Restaurer les handlers originaux
            for handler in self.original_handlers:
                self.logger.addHandler(handler)
            
            self.progress_handler = None
        
        # Réinitialiser l'état des tâches
        self._last_completed_tasks = set()
        self._active_tasks = {}
        
        if self.progress:
            self.progress = None

# Créer une instance du gestionnaire de logging
log_manager = LogManager()

def get_log_manager() -> LogManager:
    """Retourne l'instance du gestionnaire de logging."""
    return log_manager

def print_success_message(message: str) -> None:
    """
    Affiche un message de succès avec Rich ou en mode texte.
    
    Args:
        message: Message à afficher
    """
    if RICH_AVAILABLE:
        console = Console()
        console.print(f"[bold green]✓ {message}[/bold green]")
    else:
        print(f"\033[92m✓ {message}\033[0m")

def print_error_message(message: str) -> None:
    """
    Affiche un message d'erreur avec Rich ou en mode texte.
    
    Args:
        message: Message à afficher
    """
    if RICH_AVAILABLE:
        console = Console()
        console.print(f"[bold red]✗ {message}[/bold red]")
    else:
        print(f"\033[91m✗ {message}\033[0m")

def print_warning_message(message: str) -> None:
    """
    Affiche un message d'avertissement avec Rich ou en mode texte.
    
    Args:
        message: Message à afficher
    """
    if RICH_AVAILABLE:
        console = Console()
        console.print(f"[bold yellow]⚠ {message}[/bold yellow]")
    else:
        print(f"\033[93m⚠ {message}\033[0m")

def print_info_message(message: str) -> None:
    """
    Affiche un message d'information avec Rich ou en mode texte.
    
    Args:
        message: Message à afficher
    """
    if RICH_AVAILABLE:
        console = Console()
        console.print(f"[bold cyan]ℹ {message}[/bold cyan]")
    else:
        print(f"\033[96mℹ {message}\033[0m")

def format_table_reference_message(table: str, references: List[str]) -> str:
    """
    Formate un message concernant les références de tables manquantes.
    
    Args:
        table: Nom de la table
        references: Liste des tables référencées manquantes
        
    Returns:
        Le message formaté
    """
    if RICH_AVAILABLE:
        return f"[bold]{table}[/bold] fait référence à {', '.join([f'[italic]{ref}[/italic]' for ref in references])}"
    else:
        return f"{table} fait référence à {', '.join(references)}"

def print_title(title: str) -> None:
    """
    Affiche un titre de section avec Rich ou en mode texte.
    
    Args:
        title: Titre à afficher
    """
    if RICH_AVAILABLE:
        console = Console()
        console.print(f"\n[bold magenta]{title}[/bold magenta]")
        console.print("─" * len(title))
    else:
        print(f"\n\033[95m{title}\033[0m")
        print("─" * len(title))
