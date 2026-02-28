import logging
import os
from datetime import datetime
from dotenv import load_dotenv
import pymysql
import pandas as pd

# --- logging ----------------------------------------------------------------

def setup_logging(log_dir="log", log_file="etl.log"):
    """Configure les logs dans le dossier `log` (par défaut)"""
    os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(log_dir, log_file),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

# --- base de données --------------------------------------------------------

def get_connection() -> pymysql.connections.Connection:
    """Charge .env puis ouvre une connexion MySQL.

    Retourne None si la connexion échoue.
    """
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    try:
        conn = pymysql.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
        )
        logging.info("Connexion à la base de données réussie")
        return conn
    except Exception as e:
        logging.error("Erreur de connexion à la base de données : %s", e)
        return None

# --- utilitaires ----------------------------------------------------------------

def count_nulls_and_duplicates(df: pd.DataFrame):
    nulls = df.isnull().sum()
    duplicates = df.duplicated().sum()
    logging.info("Valeurs nulles par colonne :\n%s", nulls)
    logging.info("Nombre de doublons : %d", duplicates)


# petites fonctions de transformation communes, si besoin, peuvent être ajoutées ici
