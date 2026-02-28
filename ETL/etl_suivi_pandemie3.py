# etl_suivi_pandemie3.py

import argparse
import logging

import pandas as pd

from ETL.common import (
    setup_logging,
    get_connection,
    count_nulls_and_duplicates,
    transform_data,
    recup_pays_bdd,
    insert_pandemie,
    insert_suivi,
)

# colonne -> nom standard pour variole (version 2)
COLUMN_MAP = {
    'pays': 'nom_pays',
    'date': 'date_jour',
    'total_cases': 'total_cas',
    'total_deaths': 'total_mort',
    'new_cases': 'nouveau_cas',
    'new_deaths': 'nouveau_mort',
}


def main():
    setup_logging()

    parser = argparse.ArgumentParser(description="ETL vers suivi_pandemie")
    parser.add_argument("--input_file", required=True, help="Chemin du fichier CSV de données brutes")
    parser.add_argument("--virus_id", type=int, required=True, help="ID du virus à insérer dans la table pandemie")
    parser.add_argument("--nom_maladie", required=True, help="Nom de la maladie pour la table pandemie")
    parser.add_argument("--description", required=False, help="Description pour la table logging_insert")
    args = parser.parse_args()

    logging.info("Début du pipeline ETL")
    df = pd.read_csv(args.input_file)

    count_nulls_and_duplicates(df)
    country_mapping = recup_pays_bdd()
    clean_df = transform_data(df, country_mapping, COLUMN_MAP)

    conn = get_connection()
    if conn:
        id_pandemie = insert_pandemie(conn, clean_df, args.virus_id, args.nom_maladie)
        if id_pandemie:
            insert_suivi(conn, clean_df, id_pandemie, args.description)
        conn.close()

    logging.info("Pipeline ETL terminé")


if __name__ == "__main__":
    main()
