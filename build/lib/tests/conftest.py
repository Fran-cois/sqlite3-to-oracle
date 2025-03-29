"""
Fixtures pour les tests pytest du package mariadb-to-oracle.
"""

import pytest
import sqlite3
import os
import tempfile
from unittest.mock import MagicMock, patch

@pytest.fixture
def sample_sqlite_dump():
    """Fixture qui fournit un exemple de dump SQL SQLite."""
    return """
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE "users" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "name" TEXT NOT NULL,
    "email" VARCHAR(255) UNIQUE COLLATE NOCASE,
    "created_at" DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_users_email ON users (email);
INSERT INTO "users" VALUES(1,'John Doe','john@example.com','2023-01-15 10:30:45');
INSERT INTO "users" VALUES(2,'Jane Smith','jane@example.com','2023-02-20 14:25:10');
CREATE TABLE "posts" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "user_id" INTEGER NOT NULL,
    "title" TEXT NOT NULL,
    "content" TEXT,
    "published" INTEGER DEFAULT 0,
    FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);
INSERT INTO "posts" VALUES(1,1,'First Post','Hello World',1);
INSERT INTO "posts" VALUES(2,1,'Second Post','More content',0);
COMMIT;
"""

@pytest.fixture
def sample_create_table():
    """Fixture qui fournit un exemple de déclaration CREATE TABLE SQLite."""
    return """CREATE TABLE "users" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "name" TEXT NOT NULL,
    "email" VARCHAR(255) UNIQUE COLLATE NOCASE,
    "created_at" DATETIME DEFAULT CURRENT_TIMESTAMP
);"""

@pytest.fixture
def temp_sqlite_db():
    """Fixture qui crée une base de données SQLite temporaire avec des tables et des données."""
    fd, path = tempfile.mkstemp(suffix='.sqlite')
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    
    # Créer des tables de test
    cursor.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        created_at TEXT
    )
    """)
    
    cursor.execute("""
    CREATE TABLE posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        content TEXT,
        published INTEGER DEFAULT 0,
        FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
    )
    """)
    
    # Insérer des données de test
    cursor.execute("INSERT INTO users VALUES (1, 'John Doe', 'john@example.com', '2023-01-15 10:30:45')")
    cursor.execute("INSERT INTO users VALUES (2, 'Jane Smith', 'jane@example.com', '2023-02-20 14:25:10')")
    cursor.execute("INSERT INTO posts VALUES (1, 1, 'First Post', 'Hello World', 1)")
    cursor.execute("INSERT INTO posts VALUES (2, 1, 'Second Post', 'More content', 0)")
    
    conn.commit()
    conn.close()
    os.close(fd)
    
    yield path
    
    # Nettoyer après le test
    os.unlink(path)

@pytest.fixture
def mock_oracle_connection():
    """Fixture qui fournit un mock de connexion Oracle et de curseur."""
    with patch('oracledb.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        yield mock_connect, mock_conn, mock_cursor
