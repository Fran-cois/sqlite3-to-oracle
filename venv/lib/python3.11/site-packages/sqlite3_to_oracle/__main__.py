"""
Point d'entrée pour l'exécution en tant que module (-m).
Permet d'exécuter le package directement avec python -m sqlite3_to_oracle
"""

from .cli import main

if __name__ == "__main__":
    main()
