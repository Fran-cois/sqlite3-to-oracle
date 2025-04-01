"""
Tests pour le module config.py
"""
import unittest
import os
import tempfile
import json
from sqlite3_to_oracle.config import load_oracle_config, save_oracle_config, get_connection_string

class TestConfig(unittest.TestCase):
    """Tests pour les fonctions de gestion de configuration."""
    
    def setUp(self):
        """Préparation des tests."""
        # Créer un fichier de configuration temporaire
        self.temp_dir = tempfile.mkdtemp()
        self.config_file = os.path.join(self.temp_dir, "test_config.json")
        self.test_config = {
            "user": "test_user",
            "password": "test_password",
            "dsn": "test_host:1521/test_service"
        }
        
        with open(self.config_file, 'w') as f:
            json.dump(self.test_config, f)
        
        # Sauvegarder les variables d'environnement actuelles
        self.original_env = {}
        for var in ["ORACLE_ADMIN_USER", "ORACLE_ADMIN_PASSWORD", "ORACLE_ADMIN_DSN"]:
            self.original_env[var] = os.environ.get(var)
    
    def tearDown(self):
        """Nettoyage après les tests."""
        # Supprimer le fichier temporaire
        if os.path.exists(self.config_file):
            os.remove(self.config_file)
        os.rmdir(self.temp_dir)
        
        # Restaurer les variables d'environnement
        for var, value in self.original_env.items():
            if value is None:
                if var in os.environ:
                    del os.environ[var]
            else:
                os.environ[var] = value
    
    def test_load_oracle_config_from_file(self):
        """Teste le chargement de la configuration depuis un fichier."""
        config, _ = load_oracle_config(config_file=self.config_file)
        
        self.assertEqual(config["user"], self.test_config["user"])
        self.assertEqual(config["password"], self.test_config["password"])
        self.assertEqual(config["dsn"], self.test_config["dsn"])
    
    def test_load_oracle_config_from_env(self):
        """Teste le chargement de la configuration depuis les variables d'environnement."""
        # Définir des variables d'environnement pour le test
        os.environ["ORACLE_ADMIN_USER"] = "env_user"
        os.environ["ORACLE_ADMIN_PASSWORD"] = "env_password"
        os.environ["ORACLE_ADMIN_DSN"] = "env_host:1521/env_service"
        
        config, _ = load_oracle_config()
        
        self.assertEqual(config["user"], "env_user")
        self.assertEqual(config["password"], "env_password")
        self.assertEqual(config["dsn"], "env_host:1521/env_service")
    
    def test_cli_config_priority(self):
        """Teste que la configuration CLI a priorité sur les autres sources."""
        # Définir des variables d'environnement
        os.environ["ORACLE_ADMIN_USER"] = "env_user"
        os.environ["ORACLE_ADMIN_PASSWORD"] = "env_password"
        os.environ["ORACLE_ADMIN_DSN"] = "env_host:1521/env_service"
        
        # Configuration CLI
        cli_config = {
            "user": "cli_user",
            "password": "cli_password",
            "dsn": "cli_host:1521/cli_service"
        }
        
        config, _ = load_oracle_config(cli_config=cli_config, config_file=self.config_file)
        
        # La configuration CLI doit avoir priorité
        self.assertEqual(config["user"], "cli_user")
        self.assertEqual(config["password"], "cli_password")
        self.assertEqual(config["dsn"], "cli_host:1521/cli_service")
    
    def test_save_oracle_config(self):
        """Teste la sauvegarde de la configuration."""
        save_path = os.path.join(self.temp_dir, "saved_config.json")
        
        save_oracle_config(self.test_config, save_path)
        
        # Vérifier que le fichier existe
        self.assertTrue(os.path.exists(save_path))
        
        # Vérifier le contenu
        with open(save_path, 'r') as f:
            saved_config = json.load(f)
        
        self.assertEqual(saved_config["user"], self.test_config["user"])
        self.assertEqual(saved_config["password"], self.test_config["password"])
        self.assertEqual(saved_config["dsn"], self.test_config["dsn"])
    
    def test_get_connection_string(self):
        """Teste la génération de la chaîne de connexion."""
        connection_string = get_connection_string(self.test_config)
        expected = f"{self.test_config['user']}/{self.test_config['password']}@{self.test_config['dsn']}"
        
        self.assertEqual(connection_string, expected)

if __name__ == '__main__':
    unittest.main()
