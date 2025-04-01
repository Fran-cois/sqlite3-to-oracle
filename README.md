# SQLite to Oracle Converter

Outil pour convertir des bases de données SQLite vers Oracle SQL.

## Caractéristiques

- Conversion automatique des types de données SQLite vers Oracle
- Gestion des tables volumineuses et des types complexes
- Création automatique d'utilisateur Oracle
- Plusieurs modes d'importation :
  - Importation complète (schéma + données)
  - Schéma uniquement (`--schema-only`)
  - Structure relationnelle uniquement (`--only-fk-keys`) - conserve uniquement les clés primaires et étrangères

## Installation

```bash
pip install sqlite3-to-oracle
```

## Usage

```bash
# Conversion standard
sqlite3-to-oracle --sqlite_db ma_base.sqlite

# Conversion avec schéma seulement (sans données)
sqlite3-to-oracle --sqlite_db ma_base.sqlite --schema-only

# Conversion du squelette relationnel uniquement (uniquement les clés primaires et étrangères)
sqlite3-to-oracle --sqlite_db ma_base.sqlite --only-fk-keys

# Conversion avec nom d'utilisateur et mot de passe personnalisés
sqlite3-to-oracle --sqlite_db ma_base.sqlite --new-username mon_user --new-password mon_pass
```

# Options

### Options de source

```bash
# Utilisation en ligne de commande
sqlite3-to-oracle --sqlite_db path/to/your_database.sqlite

# Options supplémentaires
sqlite3-to-oracle --sqlite_db path/to/your_database.sqlite --new-username custom_user --new-password custom_pass --drop-tables --force-recreate

# Utilisation avec un fichier .env
sqlite3-to-oracle --env-file /path/to/.env
```

## Fichier .env

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

## Fonctionnalités

- Convertit les schémas et données SQLite en Oracle SQL compatible
- Crée automatiquement un utilisateur Oracle
- Exécute le script SQL généré dans Oracle
- Fournit l'URI SQLAlchemy pour se connecter à la base créée

## Configuration Oracle

Plusieurs méthodes sont disponibles pour configurer l'accès à Oracle :

1. **Variables d'environnement** :
   - ORACLE_ADMIN_USER, ORACLE_ADMIN_PASSWORD, ORACLE_ADMIN_DSN

2. **Fichier .env** :
   - Utiliser l'option --env-file pour spécifier le chemin du fichier

3. **Fichier de configuration JSON** :
   - ~/.oracle_config.json (automatique)
   - Ou spécifier avec --oracle-config-file

4. **Arguments en ligne de commande** :
   - --oracle-admin-user, --oracle-admin-password, --oracle-admin-dsn

Les paramètres de ligne de commande ont la priorité sur les autres méthodes.

## Vérifications post-importation

Après avoir importé votre base de données SQLite vers Oracle, l'outil valide automatiquement l'importation pour s'assurer que les tables, colonnes et données ont été correctement transférées.

```bash
# La validation est activée par défaut, donc cette commande effectue déjà une validation
sqlite3-to-oracle --sqlite_db path/to/your_database.sqlite

# Pour désactiver la validation (non recommandé)
sqlite3-to-oracle --sqlite_db path/to/your_database.sqlite --no-validate-schema

# Pour des détails supplémentaires sur les éventuels problèmes
sqlite3-to-oracle --sqlite_db path/to/your_database.sqlite --verbose
```

### Vérifications effectuées

La validation post-importation vérifie :

1. **Tables** : Toutes les tables SQLite sont présentes dans Oracle
2. **Schéma** : Les colonnes et leurs types de données correspondent entre SQLite et Oracle  
3. **Données** : Le nombre d'enregistrements dans chaque table correspond entre les deux bases
4. **Récapitulatif** : Un résumé statistique avec le total des tables, colonnes et lignes

### Interpréter les résultats

- ✅ **VALIDATION RÉUSSIE** : Toutes les tables, colonnes et données ont été correctement importées
- ⚠️ **VALIDATION AVEC AVERTISSEMENTS** : L'importation est partiellement réussie mais présente des problèmes
  
Utilisez l'option `--verbose` pour voir les détails spécifiques des avertissements et des erreurs, comme les tables ou colonnes manquantes.

## Traitement par lots

L'outil permet également de traiter plusieurs bases de données SQLite en une seule fois :

```bash
# Traiter tous les fichiers .sqlite dans un répertoire
sqlite3-to-oracle --batch --sqlite-dir /chemin/vers/repertoire --uri-output-file uris.txt

# Traiter des fichiers spécifiques avec un motif
sqlite3-to-oracle --batch --sqlite-dir /chemin/vers/repertoire --file-pattern "data_*.db" --uri-output-file uris.txt
```

Les URIs SQLAlchemy de toutes les bases importées avec succès seront enregistrées dans le fichier spécifié.

### Utilisateurs par base de données

En mode batch, chaque base de données SQLite est importée avec son propre utilisateur Oracle dédié. Le nom d'utilisateur et le mot de passe sont dérivés automatiquement du nom du fichier SQLite:

- Pour un fichier `clients.sqlite` → utilisateur Oracle `clients`
- Pour un fichier `sales_2023.db` → utilisateur Oracle `sales2023`

Cela permet une isolation complète des données entre les différentes bases importées.

> **Note:** Pour utiliser un seul utilisateur administrateur pour toutes les importations, ajoutez l'option `--use-admin-user`.

### Options de traitement par lots

| Option | Description |
|--------|-------------|
| `--batch` | Activer le mode de traitement par lots |
| `--sqlite-dir` | Répertoire contenant les fichiers SQLite à importer |
| `--file-pattern` | Motif de fichiers à traiter (par défaut: *.sqlite) |
| `--uri-output-file` | Fichier pour enregistrer les URIs SQLAlchemy |
| `--continue-on-error` | Continuer le traitement même en cas d'erreur |
| `--use-admin-user` | Utiliser un seul utilisateur (admin) pour toutes les bases |