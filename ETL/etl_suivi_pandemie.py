# etl_suivi_pandemie.py

import argparse
import logging
from datetime import datetime

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

# colonne -> nom standard pour covid (version 1)
COLUMN_MAP = {
    'Date': 'date_jour',
    'Country/Region': 'nom_pays',
    'Confirmed': 'total_cas',
    'Deaths': 'total_mort',
    'Recovered': 'guerison',
    'New cases': 'nouveau_cas',
    'New deaths': 'nouveau_mort',
    'New recovered': 'nouvelle_guerison',
}


def main():
    setup_logging()

    parser = argparse.ArgumentParser(description="ETL vers suivi_pandemie")
    parser.add_argument("--input_file", required=True, help="Chemin vers fichier CSV data-sets")
    parser.add_argument("--virus_id", type=int, required=True, help="ID du virus pour la table pandemie 1=covid 2=variole")
    parser.add_argument("--nom_maladie", required=True, help="Nom de la maladie pour la table pandemie ex: COVID-19")
    parser.add_argument("--description", required=False, help="Plus d'infos sur l'insertion de suivi pandemie")
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
            insert_suivi(conn, clean_df, id_pandemie, args.description, extra_cols=['guerison','nouvelle_guerison'])
        conn.close()

    logging.info("Pipeline ETL terminé")


if __name__ == "__main__":
    main()
