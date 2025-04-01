FROM python:3.10-slim

WORKDIR /app

# Installer les dépendances système nécessaires
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libaio1 \
    && rm -rf /var/lib/apt/lists/*

# Installer le client instantané Oracle (version slim)
RUN mkdir -p /opt/oracle
WORKDIR /opt/oracle

# Préparer l'environnement Oracle
ENV ORACLE_HOME=/opt/oracle
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME
ENV PATH=$PATH:$ORACLE_HOME

# Copier le projet
WORKDIR /app
COPY . /app/

# Créer les répertoires nécessaires pour les données
RUN mkdir -p /data/input /data/output

# Installer les dépendances Python
RUN pip install --no-cache-dir -e ".[ui]"

# Exposer le répertoire /data pour monter les volumes
VOLUME ["/data"]

# Commande par défaut
ENTRYPOINT ["sqlite3-to-oracle"]
CMD ["--help"]
