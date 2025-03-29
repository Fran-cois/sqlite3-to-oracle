#!/bin/bash

# Exemple de script pour tester les connexions Oracle

# Tester avec des arguments en ligne de commande
echo "Test avec arguments en ligne de commande:"
sqlite3-to-oracle --validate-credentials-only \
  --oracle-admin-user system \
  --oracle-admin-password manager \
  --oracle-admin-dsn localhost:1521/free \
  --new-username test_user \
  --new-password test_pass

# Tester avec un fichier .env
echo -e "\nTest avec un fichier .env:"
cat > .env.test << EOL
ORACLE_ADMIN_USER=system
ORACLE_ADMIN_PASSWORD=manager
ORACLE_ADMIN_DSN=localhost:1521/free
ORACLE_NEW_USERNAME=env_user
ORACLE_NEW_PASSWORD=env_pass
EOL

sqlite3-to-oracle --validate-credentials-only --env-file .env.test

# Nettoyer
rm .env.test
