"""
Script de diagnostic pour la connexion Oracle.
Ce script effectue une série de tests pour vérifier la connexion 
et afficher des informations détaillées sur la base de données.
"""

import sys
import os
import argparse
from typing import Dict, Optional, List

# Essayer d'importer oracledb
try:
    import oracledb
except ImportError:
    print("Erreur: Bibliothèque oracledb non installée.")
    print("Installez-la avec: pip install oracledb")
    sys.exit(1)

def parse_args():
    """Analyse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(description="Outil de diagnostic pour la connexion Oracle")
    parser.add_argument("--user", help="Nom d'utilisateur Oracle")
    parser.add_argument("--password", help="Mot de passe Oracle")
    parser.add_argument("--dsn", help="DSN Oracle (format: host:port/service)")
    parser.add_argument("--tns", help="Chaîne de connexion TNS complète (alternative au DSN)")
    
    return parser.parse_args()

def get_connection_params(args) -> Dict[str, str]:
    """Récupère les paramètres de connexion depuis les arguments ou les variables d'environnement."""
    params = {}
    
    # Priorité aux arguments
    if args.user:
        params["user"] = args.user
    elif os.environ.get("ORACLE_USER"):
        params["user"] = os.environ.get("ORACLE_USER")
    elif os.environ.get("ORACLE_ADMIN_USER"):
        params["user"] = os.environ.get("ORACLE_ADMIN_USER")
    else:
        params["user"] = input("Nom d'utilisateur Oracle: ")
    
    if args.password:
        params["password"] = args.password
    elif os.environ.get("ORACLE_PASSWORD"):
        params["password"] = os.environ.get("ORACLE_PASSWORD")
    elif os.environ.get("ORACLE_ADMIN_PASSWORD"):
        params["password"] = os.environ.get("ORACLE_ADMIN_PASSWORD")
    else:
        import getpass
        params["password"] = getpass.getpass("Mot de passe Oracle: ")
    
    if args.dsn:
        params["dsn"] = args.dsn
    elif args.tns:
        params["dsn"] = args.tns
    elif os.environ.get("ORACLE_DSN"):
        params["dsn"] = os.environ.get("ORACLE_DSN")
    elif os.environ.get("ORACLE_ADMIN_DSN"):
        params["dsn"] = os.environ.get("ORACLE_ADMIN_DSN")
    else:
        params["dsn"] = input("DSN Oracle (format host:port/service): ")
    
    return params

def test_connection(params: Dict[str, str]) -> bool:
    """Teste la connexion à Oracle et affiche des informations détaillées."""
    print("\n=== TEST DE CONNEXION ORACLE ===")
    print(f"Tentative de connexion avec l'utilisateur {params['user']} sur {params['dsn']}")
    
    try:
        conn = oracledb.connect(
            user=params["user"],
            password=params["password"],
            dsn=params["dsn"]
        )
        print("✓ Connexion réussie!")
        
        # Informations de base
        cursor = conn.cursor()
        cursor.execute("SELECT BANNER FROM V$VERSION")
        version = cursor.fetchone()[0]
        print(f"Version Oracle: {version}")
        
        cursor.execute("SELECT SYS_CONTEXT('USERENV', 'DB_NAME') FROM DUAL")
        db_name = cursor.fetchone()[0]
        print(f"Nom de la base: {db_name}")
        
        cursor.execute("SELECT SYS_CONTEXT('USERENV', 'INSTANCE_NAME') FROM DUAL")
        instance = cursor.fetchone()[0]
        print(f"Instance: {instance}")
        
        cursor.execute("SELECT USER FROM DUAL")
        current_user = cursor.fetchone()[0]
        print(f"Utilisateur connecté: {current_user}")
        
        # Privilèges
        print("\n=== PRIVILÈGES UTILISATEUR ===")
        try:
            cursor.execute("SELECT * FROM SESSION_PRIVS")
            privileges = [row[0] for row in cursor.fetchall()]
            print(f"Nombre de privilèges: {len(privileges)}")
            
            # Privilèges importants à vérifier
            key_privileges = [
                "CREATE SESSION", "CREATE USER", "CREATE TABLE", 
                "CREATE VIEW", "CREATE SEQUENCE", "UNLIMITED TABLESPACE"
            ]
            
            for priv in key_privileges:
                if priv in privileges:
                    print(f"✓ {priv}: Présent")
                else:
                    print(f"✗ {priv}: Absent")
            
            print("\nAutres privilèges:")
            other_privs = [p for p in privileges if p not in key_privileges]
            for i in range(0, len(other_privs), 3):
                print("  " + ", ".join(other_privs[i:i+3]))
            
        except Exception as e:
            print(f"Erreur lors de la récupération des privilèges: {str(e)}")
        
        # Rôles
        print("\n=== RÔLES ATTRIBUÉS ===")
        try:
            cursor.execute("SELECT * FROM USER_ROLE_PRIVS")
            roles = [row[0] for row in cursor.fetchall()]
            if roles:
                for role in roles:
                    print(f"Rôle: {role}")
            else:
                print("Aucun rôle attribué")
        except Exception as e:
            print(f"Erreur lors de la récupération des rôles: {str(e)}")
        
        # Tablespaces
        print("\n=== TABLESPACES DISPONIBLES ===")
        try:
            for view in ["USER_TABLESPACES", "DBA_TABLESPACES", "ALL_TABLESPACES"]:
                try:
                    cursor.execute(f"SELECT TABLESPACE_NAME FROM {view}")
                    tablespaces = [row[0] for row in cursor.fetchall()]
                    print(f"Tablespaces (via {view}):")
                    for ts in tablespaces:
                        print(f"  - {ts}")
                    break
                except:
                    continue
            else:
                print("Impossible d'accéder aux vues de tablespaces")
        except Exception as e:
            print(f"Erreur lors de la récupération des tablespaces: {str(e)}")
        
        cursor.close()
        conn.close()
        return True
        
    except oracledb.DatabaseError as e:
        error, = e.args
        error_code = getattr(error, 'code', 'N/A')
        error_message = getattr(error, 'message', str(error))
        
        print(f"\n✗ Échec de la connexion: Erreur Oracle {error_code}")
        print(f"Message: {error_message}")
        
        # Suggestions selon le code d'erreur
        if "ORA-01017" in str(error):
            print("\nSuggestion: Les identifiants sont incorrects. Vérifiez:")
            print("- Le nom d'utilisateur est valide et existe dans la base de données")
            print("- Le mot de passe est correct")
            print("- L'utilisateur n'est pas verrouillé (demandez à un DBA)")
        
        elif "ORA-12541" in str(error):
            print("\nSuggestion: Le serveur Oracle n'est pas accessible. Vérifiez:")
            print("- Le serveur Oracle est démarré")
            print("- Le listener Oracle est actif")
            print("- Le pare-feu n'empêche pas la connexion")
            print("- L'adresse et le port sont corrects")
        
        elif "ORA-12514" in str(error):
            print("\nSuggestion: Le service spécifié n'existe pas. Vérifiez:")
            print("- Le nom du service est correct")
            print("- Le format du DSN est correct (host:port/service)")
            print("- La base de données est démarrée")
        
        else:
            print("\nSuggestion: Vérifiez les paramètres de connexion et l'état du serveur Oracle")
        
        return False
    
    except Exception as e:
        print(f"\n✗ Erreur inattendue: {str(e)}")
        print(f"Type d'erreur: {type(e).__name__}")
        return False

def main():
    """Point d'entrée principal."""
    args = parse_args()
    params = get_connection_params(args)
    success = test_connection(params)
    
    if success:
        print("\n✓ Diagnostic complet: La connexion Oracle fonctionne correctement.")
    else:
        print("\n✗ Diagnostic complet: La connexion Oracle a échoué.")
        sys.exit(1)

if __name__ == "__main__":
    main()
