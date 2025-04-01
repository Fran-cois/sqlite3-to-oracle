#!/bin/bash

# Script pour exécuter sqlite3-to-oracle en mode batch

# Définir le chemin vers le fichier .env
ENV_FILE="batch_mode.env"

# Vérifier que le fichier existe
if [ ! -f "$ENV_FILE" ]; then
    echo "Erreur: Le fichier $ENV_FILE n'existe pas."
    exit 1
fi

# Exécuter sqlite3-to-oracle en mode batch
echo "Démarrage de l'importation en mode batch..."
sqlite3-to-oracle --batch --env-file "$ENV_FILE" --verbose

# Vérifier le code de sortie
if [ $? -eq 0 ]; then
    echo "Importation en mode batch terminée avec succès!"
    
    # Afficher le fichier URI si disponible
    URI_FILE=$(grep ORACLE_URI_OUTPUT_FILE "$ENV_FILE" | cut -d'=' -f2)
    if [ -f "$URI_FILE" ]; then
        echo "URIs générées:"
        cat "$URI_FILE"
    fi
else
    echo "Erreur lors de l'importation en mode batch."
    exit 1
fi
