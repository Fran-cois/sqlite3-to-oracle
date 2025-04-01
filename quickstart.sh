#!/bin/bash

# Script de démarrage rapide pour SQLite vers Oracle
# Ce script guide l'utilisateur à travers le processus de configuration et d'exécution

echo "====================================================="
echo "    SQLite vers Oracle - Assistant de Démarrage      "
echo "====================================================="

# Vérifier que Python est installé
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 n'est pas installé ou n'est pas dans le PATH."
    echo "   Veuillez installer Python 3.7+ et réessayer."
    exit 1
fi

# Vérifier si oracledb est installé
if ! python3 -c "import oracledb" &> /dev/null; then
    echo "ℹ️ Le module oracledb n'est pas installé. Installation en cours..."
    pip install oracledb
fi

# Vérifier si le package est installé
if ! python3 -c "import sqlite3_to_oracle" &> /dev/null; then
    echo "ℹ️ SQLite vers Oracle n'est pas installé. Installation en cours..."
    pip install -e .
fi

# Demander les informations de connexion Oracle
echo
echo "Configuration de la connexion Oracle:"
echo "-----------------------------------"
read -p "Nom d'utilisateur Oracle (par défaut: system): " oracle_user
oracle_user=${oracle_user:-system}

read -s -p "Mot de passe Oracle: " oracle_password
echo

read -p "DSN Oracle (format: host:port/service) (par défaut: localhost:1521/XEPDB1): " oracle_dsn
oracle_dsn=${oracle_dsn:-localhost:1521/XEPDB1}

# Enregistrer les paramètres dans .env
cat > .env << EOF
# Configuration de connexion Oracle
ORACLE_ADMIN_USER=${oracle_user}
ORACLE_ADMIN_PASSWORD=${oracle_password}
ORACLE_ADMIN_DSN=${oracle_dsn}
EOF

echo "✅ Configuration enregistrée dans .env"

# Demander le chemin de la base SQLite
echo
echo "Sélection de la base SQLite à convertir:"
echo "--------------------------------------"
read -p "Chemin vers la base SQLite: " sqlite_path

if [ ! -f "$sqlite_path" ]; then
    echo "❌ Le fichier $sqlite_path n'existe pas."
    exit 1
fi

# Demander les options supplémentaires
echo
echo "Options de conversion:"
echo "-------------------"

read -p "Créer un nouvel utilisateur Oracle? (o/n, défaut: o): " create_user
create_user=${create_user:-o}

if [ "$create_user" = "o" ]; then
    read -p "Nom d'utilisateur pour le nouvel utilisateur (par défaut: basé sur le nom du fichier): " new_username
    
    if [ -n "$new_username" ]; then
        user_option="--new-username $new_username"
        read -s -p "Mot de passe pour le nouvel utilisateur: " new_password
        echo
        if [ -n "$new_password" ]; then
            user_option="$user_option --new-password $new_password"
        fi
    else
        user_option=""
    fi
else
    user_option="--use-admin-user"
fi

read -p "Supprimer les tables existantes si présentes? (o/n, défaut: n): " drop_tables
drop_tables=${drop_tables:-n}
if [ "$drop_tables" = "o" ]; then
    drop_option="--drop-tables"
else
    drop_option=""
fi

read -p "Importer uniquement le schéma (pas les données)? (o/n, défaut: n): " schema_only
schema_only=${schema_only:-n}
if [ "$schema_only" = "o" ]; then
    schema_option="--schema-only"
else
    schema_option=""
fi

read -p "Utiliser VARCHAR2 pour tous les types numériques problématiques? (o/n, défaut: o): " use_varchar
use_varchar=${use_varchar:-o}
if [ "$use_varchar" = "o" ]; then
    varchar_option="--use-varchar"
else
    varchar_option=""
fi

read -p "Mode verbeux (plus de détails)? (o/n, défaut: n): " verbose
verbose=${verbose:-n}
if [ "$verbose" = "o" ]; then
    verbose_option="--verbose"
else
    verbose_option=""
fi

# Exécuter la commande
echo
echo "📝 Commande à exécuter:"
echo "sqlite3-to-oracle --sqlite_db \"$sqlite_path\" --env-file .env $user_option $drop_option $schema_option $varchar_option $verbose_option"
echo

read -p "Exécuter maintenant? (o/n, défaut: o): " run_now
run_now=${run_now:-o}

if [ "$run_now" = "o" ]; then
    echo "🚀 Démarrage de la conversion..."
    sqlite3-to-oracle --sqlite_db "$sqlite_path" --env-file .env $user_option $drop_option $schema_option $varchar_option $verbose_option
else
    echo "ℹ️ Vous pouvez exécuter la commande manuellement plus tard."
fi

echo
echo "Merci d'avoir utilisé l'assistant de démarrage SQLite vers Oracle!"
echo "Pour plus d'options et d'informations, consultez la documentation: README.md"
