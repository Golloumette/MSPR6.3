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


def transform_data(df: pd.DataFrame, country_mapping: dict, rename_map: dict) -> pd.DataFrame:
    """Effectue les opérations de transformation communes aux ETL.

    - renomme les colonnes à partir de ``rename_map``
    - supprime les doublons
    - met les noms de colonnes en minuscules
    - remplace les nulls numériques par la médiane
    - convertit les négatifs en positifs
    - ajoute la colonne ``pays_id`` via le mapping fourni
    - force ``date_jour`` en type date
    """
    df = df.rename(columns=rename_map)
    df = df.drop_duplicates()
    df.columns = [col.lower().strip() for col in df.columns]

    num_cols = df.select_dtypes(include=["number"]).columns
    for col in num_cols:
        if df[col].isnull().any():
            median_val = df[col].median()
            df[col].fillna(median_val, inplace=True)
            logging.info(f"Valeurs nulles dans '{col}' remplacées par la médiane {median_val}")

    for col in num_cols:
        negative_count = (df[col] < 0).sum()
        if negative_count > 0:
            logging.info(
                "%d valeurs négatives détectées dans '%s' ➜ conversion en positif", negative_count, col
            )
            df[col] = df[col].abs()

    df['pays_id'] = df['nom_pays'].map(country_mapping)
    df = df.dropna(subset=['pays_id'])
    df['pays_id'] = df['pays_id'].astype(int)
    df['date_jour'] = pd.to_datetime(df['date_jour']).dt.date

    return df


def recup_pays_bdd(conn=None) -> dict:
    """Retourne un dictionnaire nom->id de la table pays.

    Si ``conn`` est None, la connexion est ouverte puis fermée automatiquement.
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True
    mapping = {}
    if conn is None:
        return mapping

    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT nom, id FROM pays")
        rows = cursor.fetchall()
        mapping = {nom: id_ for nom, id_ in rows}
        logging.info(f"Mapping pays chargé : {mapping}")
    except Exception as e:
        logging.error(f"Erreur lors du chargement des pays : {e}")
    finally:
        if cursor:
            cursor.close()
        if close_conn and conn:
            conn.close()
    return mapping


def insert_pandemie(conn, df, virus_id, nom_maladie):
    """Insère une entrée dans la table ``pandemie`` et retourne l'ID généré."""
    cursor = conn.cursor()
    try:
        date_debut = df['date_jour'].min()
        date_fin = df['date_jour'].max()
        cursor.execute("""
            INSERT INTO pandemie (virus_id, nom_maladie, date_apparition, date_fin)
            VALUES (%s, %s, %s, %s)
        """, (virus_id, nom_maladie, date_debut, date_fin))
        conn.commit()
        id_pandemie = cursor.lastrowid
        logging.info(f"Pandémie insérée avec ID {id_pandemie}, maladie : {nom_maladie}, virus ID : {virus_id}, début : {date_debut}, fin : {date_fin}")
        return id_pandemie
    except Exception as e:
        logging.error(f"Erreur lors de l'insertion dans pandemie : {e}")
        return None
    finally:
        cursor.close()


def insert_suivi(conn, df, id_pandemie, description, extra_cols=None):
    """Insère les lignes de suivi dans la base via ``suivi_pandemie``.

    ``extra_cols`` est une liste des colonnes supplémentaires à écrire
    après ``total_cas, total_mort, nouveau_cas, nouveau_mort``.
    """
    if extra_cols is None:
        extra_cols = []
    cursor = conn.cursor()
    try:
        now = datetime.now()
        cursor.execute(
            "INSERT INTO logging_insert (date_insertion, description) VALUES (%s, %s)",
            (now, description),
        )
        conn.commit()
        id_logging = cursor.lastrowid

        cols = ['total_cas', 'total_mort', 'nouveau_cas', 'nouveau_mort'] + extra_cols
        placeholders = ', '.join(['%s'] * (4 + len(extra_cols)))
        sql = f"""
            INSERT INTO suivi_pandemie (id_logging, id_pandemie, pays_id, date_jour,{', '.join(cols)})
            VALUES (%s, %s, %s, %s, {placeholders})
        """
        for _, row in df.iterrows():
            values = [
                id_logging,
                id_pandemie,
                row['pays_id'],
                row['date_jour'],
            ] + [row[col] for col in cols]
            cursor.execute(sql, tuple(values))
        conn.commit()
        logging.info(f"{len(df)} lignes insérées dans suivi_pandemie avec id_logging = {id_logging} et description = '{description}'")
    except Exception as e:
        logging.error(f"Erreur MySQL : {e}")
    finally:
        cursor.close()
