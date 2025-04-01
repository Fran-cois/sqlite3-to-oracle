"""
Tests pour le module oracle_utils.py
"""

import pytest
import os
import tempfile
from unittest.mock import patch, mock_open, MagicMock
from sqlite3_to_oracle.oracle_utils import (
    create_oracle_user,
    execute_sql_file,
    get_sqlalchemy_uri
)

class TestCreateOracleUser:
    """Tests pour la fonction create_oracle_user."""
    
    def test_creates_user_successfully(self, mock_oracle_connection):
        """Teste la création réussie d'un utilisateur Oracle."""
        mock_connect, mock_conn, mock_cursor = mock_oracle_connection
        
        admin_config = {
            "user": "system",
            "password": "manager",
            "dsn": "localhost:1521/xe"
        }
        
        create_oracle_user(admin_config, "test_user", "test_pass")
        
        # Vérifier que les bonnes requêtes ont été exécutées
        mock_connect.assert_called_once_with(
            user="system", password="manager", dsn="localhost:1521/xe"
        )
        mock_cursor.execute.assert_any_call("CREATE USER test_user IDENTIFIED BY test_pass")
        mock_cursor.execute.assert_any_call("GRANT CONNECT, RESOURCE TO test_user")
        mock_conn.commit.assert_called_once()
    
    def test_handles_existing_user(self, mock_oracle_connection):
        """Teste la gestion d'un utilisateur qui existe déjà."""
        mock_connect, mock_conn, mock_cursor = mock_oracle_connection
        
        # Simuler une erreur ORA-01920 (utilisateur existe déjà)
        error = MagicMock()
        error.__str__ = lambda self: "ORA-01920: user name 'TEST_USER' conflicts with another user or role name"
        mock_cursor.execute.side_effect = [
            type('OracleError', (Exception,), {'args': (error,)})(),
            None,  # pour GRANT
            None   # pour ALTER USER
        ]
        
        admin_config = {
            "user": "system",
            "password": "manager",
            "dsn": "localhost:1521/xe"
        }
        
        # Ne devrait pas lever d'exception
        create_oracle_user(admin_config, "test_user", "test_pass")
        
        # Vérifier que les bonnes requêtes ont été exécutées
        mock_connect.assert_called_once()
        assert mock_cursor.execute.call_count >= 2
        mock_conn.commit.assert_called_once()

class TestExecuteSqlFile:
    """Tests pour la fonction execute_sql_file."""
    
    @patch('builtins.open', new_callable=mock_open, read_data="CREATE TABLE test (id NUMBER);")
    def test_executes_sql_file(self, mock_file, mock_oracle_connection):
        """Teste l'exécution d'un fichier SQL."""
        mock_connect, mock_conn, mock_cursor = mock_oracle_connection
        
        # Simuler que user_objects renvoie une liste vide
        mock_cursor.fetchall.return_value = []
        
        user_config = {
            "user": "test_user",
            "password": "test_pass",
            "dsn": "localhost:1521/xe"
        }
        
        execute_sql_file(user_config, "test.sql")
        
        # Vérifier que la connexion a été établie
        mock_connect.assert_called_once_with(
            user="test_user", password="test_pass", dsn="localhost:1521/xe"
        )
        
        # Vérifier que le fichier a été ouvert
        mock_file.assert_called_once_with("test.sql", "r")
        
        # Vérifier que des commandes SQL ont été exécutées
        assert mock_cursor.execute.call_count > 0
        mock_conn.commit.assert_called_once()

class TestGetSqlalchemyUri:
    """Tests pour la fonction get_sqlalchemy_uri."""
    
    def test_builds_uri_correctly(self):
        """Vérifie que l'URI SQLAlchemy est correctement construit."""
        config = {
            "user": "test_user",
            "password": "test_pass",
            "dsn": "localhost:1521/xe"
        }
        
        uri = get_sqlalchemy_uri(config)
        expected = "oracle+oracledb://test_user:test_pass@localhost:1521/xe"
        assert uri == expected
    
    def test_handles_dsn_without_service_name(self):
        """Vérifie que l'URI est correctement construit même sans service_name."""
        config = {
            "user": "test_user",
            "password": "test_pass",
            "dsn": "localhost:1521"
        }
        
        uri = get_sqlalchemy_uri(config)
        expected = "oracle+oracledb://test_user:test_pass@localhost:1521/"
        assert uri == expected
    
    def test_handles_dsn_without_port(self):
        """Vérifie que l'URI est correctement construit même sans port."""
        config = {
            "user": "test_user",
            "password": "test_pass",
            "dsn": "localhost/xe"
        }
        
        uri = get_sqlalchemy_uri(config)
        expected = "oracle+oracledb://test_user:test_pass@localhost:1521/xe"
        assert uri == expected
