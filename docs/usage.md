# SQLite to Oracle Converter - Guide d'utilisation

Ce guide explique comment utiliser l'outil `sqlite3-to-oracle` pour convertir des bases de données SQLite vers Oracle.

## Installation

```bash
pip install sqlite3-to-oracle
```

## Utilisation de base

La commande la plus simple pour convertir une base de données SQLite vers Oracle est :

```bash
sqlite3-to-oracle --sqlite_db chemin/vers/votre_base.sqlite
```

Cette commande va :
1. Extraire le schéma et les données de la base SQLite
2. Convertir le SQL SQLite en SQL Oracle compatible
3. Créer un utilisateur Oracle (nom basé sur le nom du fichier SQLite)
4. Exécuter le script SQL pour créer les tables et insérer les données
5. Afficher les informations de connexion SQLAlchemy

## Options importantes

### Options de source SQLite

| Option | Description |
|--------|-------------|
| `--sqlite_db FILE` | Chemin vers le fichier de base de données SQLite |
| `--output-file FILE` | Nom du fichier SQL de sortie (par défaut: nom_base_oracle.sql) |

### Options de cible Oracle

| Option | Description |
|--------|-------------|
| `--new-username USER` | Nom du nouvel utilisateur Oracle à créer |
| `--new-password PASS` | Mot de passe du nouvel utilisateur Oracle |
| `--drop-tables` | Supprimer les tables existantes avant de les recréer |
| `--force-recreate` | Supprimer et recréer l'utilisateur Oracle et tous ses objets |
| `--schema-only` | Convertir uniquement le schéma, sans les données |

### Options de validation du schéma

| Option | Description |
|--------|-------------|
| `--validate-schema` | Valider le schéma et les données après l'importation (activé par défaut) |
| `--no-validate-schema` | Désactiver la validation du schéma après l'importation |

### Options d'administration Oracle

| Option | Description |
|--------|-------------|
| `--oracle-admin-user USER` | Nom d'utilisateur administrateur Oracle |
| `--oracle-admin-password PASS` | Mot de passe administrateur Oracle |
| `--oracle-admin-dsn DSN` | DSN Oracle (format: host:port/service) |
| `--oracle-config-file FILE` | Fichier de configuration Oracle (format JSON) |
| `--env-file FILE` | Fichier .env contenant les variables d'environnement |
| `--check-connection-only` | Vérifier uniquement la connexion Oracle sans effectuer de conversion |
| `--validate-credentials-only` | Vérifier uniquement les identifiants Oracle sans effectuer de conversion |

### Options de logging

| Option | Description |
|--------|-------------|
| `--verbose`, `-v` | Activer les messages de débogage détaillés |
| `--quiet`, `-q` | Afficher uniquement les erreurs (mode silencieux) |

## Utilisation avec un fichier .env

Vous pouvez configurer toutes les options dans un fichier .env :

```env
# Configuration Oracle Admin
ORACLE_ADMIN_USER=system
ORACLE_ADMIN_PASSWORD=password
ORACLE_ADMIN_DSN=localhost:1521/free

# Options du programme
ORACLE_SQLITE_DB=/path/to/database.sqlite
ORACLE_OUTPUT_FILE=/path/to/output.sql
ORACLE_NEW_USERNAME=new_user
ORACLE_NEW_PASSWORD=new_pass
ORACLE_DROP_TABLES=true
ORACLE_FORCE_RECREATE=true
ORACLE_SCHEMA_ONLY=false
```

Et l'utiliser avec :

```bash
sqlite3-to-oracle --env-file /path/to/.env
```

## Diagnostiquer les problèmes

### Tester la connexion Oracle

Pour vérifier uniquement la connexion à Oracle sans effectuer de conversion :

```bash
sqlite3-to-oracle --check-connection-only --oracle-admin-user system --oracle-admin-password manager --oracle-admin-dsn localhost:1521/XEPDB1
```

### Valider les identifiants

Pour vérifier à la fois les identifiants admin et ceux du nouvel utilisateur :

```bash
sqlite3-to-oracle --validate-credentials-only --oracle-admin-user system --oracle-admin-password manager --oracle-admin-dsn localhost:1521/XEPDB1 --new-username test_user --new-password test_pass
```
