"""
Tests pour le module converter.py
"""
import unittest
import re
from sqlite3_to_oracle.converter import (
    sanitize_sql_value, 
    filter_sqlite_specific_statements,
    validate_numeric_precision,
    convert_date_format
)

class TestConverter(unittest.TestCase):
    """Tests pour les fonctions de conversion SQLite vers Oracle."""
    
    def test_sanitize_sql_value(self):
        """Teste la fonction sanitize_sql_value."""
        # Test avec None
        self.assertEqual(sanitize_sql_value(None), "NULL")
        
        # Test avec nombre
        self.assertEqual(sanitize_sql_value(42), "42")
        self.assertEqual(sanitize_sql_value(3.14), "3.14")
        
        # Test avec chaîne
        self.assertEqual(sanitize_sql_value("abc"), "'abc'")
        
        # Test avec apostrophes
        self.assertEqual(sanitize_sql_value("O'Reilly"), "'O''Reilly'")
        
        # Test avec caractères spéciaux
        self.assertEqual(sanitize_sql_value("line1\nline2"), "'line1 line2'")
        
        # Test avec booléen
        self.assertEqual(sanitize_sql_value(True), "1")
        self.assertEqual(sanitize_sql_value(False), "0")
    
    def test_filter_sqlite_specific_statements(self):
        """Teste la fonction filter_sqlite_specific_statements."""
        # SQL avec commandes spécifiques SQLite
        sql = """
        PRAGMA foreign_keys=OFF;
        BEGIN TRANSACTION;
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users VALUES (1, 'Alice');
        COMMIT;
        VACUUM;
        """
        
        # Le résultat attendu ne doit pas contenir PRAGMA, BEGIN TRANSACTION, COMMIT ni VACUUM
        expected = """

        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users VALUES (1, 'Alice');


        """
        
        result = filter_sqlite_specific_statements(sql)
        self.assertEqual(result, expected)
    
    def test_validate_numeric_precision(self):
        """Teste la fonction validate_numeric_precision."""
        # Précision dans la plage valide
        self.assertEqual(validate_numeric_precision("NUMBER(10,2)"), "NUMBER(10,2)")
        
        # Précision trop grande
        self.assertEqual(validate_numeric_precision("NUMBER(40,5)"), "NUMBER(38,5)")
        
        # Échelle plus grande que précision
        self.assertEqual(validate_numeric_precision("NUMBER(10,15)"), "NUMBER(10,10)")
        
        # Type non NUMBER
        self.assertEqual(validate_numeric_precision("VARCHAR2(100)"), "VARCHAR2(100)")
    
    def test_convert_date_format(self):
        """Teste la fonction convert_date_format."""
        # Format ISO standard
        self.assertEqual(
            convert_date_format("2023-01-15"), 
            "TO_DATE('2023-01-15', 'YYYY-MM-DD')"
        )
        
        # Format avec heure
        date_with_time = convert_date_format("2023-01-15 14:30:00")
        self.assertTrue(
            re.match(r"TO_DATE\('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', 'YYYY-MM-DD HH24:MI:SS'\)", date_with_time)
        )
        
        # Format américain
        self.assertTrue(
            re.match(r"TO_DATE\('\d{4}-\d{2}-\d{2}', 'YYYY-MM-DD'\)", convert_date_format("01/15/2023"))
        )
        
        # Valeur non date
        self.assertEqual(convert_date_format("not a date"), "SYSDATE")

if __name__ == '__main__':
    unittest.main()
