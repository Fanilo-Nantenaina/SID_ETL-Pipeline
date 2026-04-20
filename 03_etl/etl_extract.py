import pandas as pd
from datetime import datetime
from etl_config import ENGINE_OLTP, ENGINE_STG
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)


def _add_stg_cols(df):
    df["stg_loaded"] = datetime.now()
    df["stg_status"] = "PENDING"
    return df


def extract_all():

    for table in ["familles", "produits", "clients", "commerciaux", "modes_reglement"]:
        df = pd.read_sql(f"SELECT * FROM {table}", ENGINE_OLTP)
        df = _add_stg_cols(df)
        df.to_sql(f"stg_{table}", ENGINE_STG, if_exists="replace", index=False)
        log.info(f"  {table} → {len(df)} lignes extraites")

    df_lignes = pd.read_sql(
        """
        SELECT lf.*,
               f.date_facture,
               f.id_client,
               f.id_commercial
        FROM lignes_facture lf
        JOIN factures f ON f.id_facture = lf.id_facture
        """,
        ENGINE_OLTP,
    )
    df_lignes = _add_stg_cols(df_lignes)
    df_lignes.to_sql("stg_lignes_facture", ENGINE_STG, if_exists="replace", index=False)
    log.info(f"  lignes_facture → {len(df_lignes)} lignes extraites")

    df_regl = pd.read_sql("SELECT * FROM reglements", ENGINE_OLTP)
    df_regl = _add_stg_cols(df_regl)
    df_regl.to_sql("stg_reglements", ENGINE_STG, if_exists="replace", index=False)
    log.info(f"  reglements → {len(df_regl)} lignes extraites")


if __name__ == "__main__":
    extract_all()
