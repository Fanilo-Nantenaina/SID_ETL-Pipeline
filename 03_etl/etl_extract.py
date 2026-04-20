import pandas as pd
from etl_config import ENGINE_OLTP, ENGINE_STG
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)


def extract_all():
    """Extrait toutes les tables OLTP vers le staging."""

    # 1. Tables de référence (full load)
    for table in ["familles", "produits", "clients", "commerciaux", "modes_reglement"]:
        df = pd.read_sql(f"SELECT * FROM {table}", ENGINE_OLTP)
        df.to_sql(f"stg_{table}", ENGINE_STG, if_exists="replace", index=False)
        log.info(f"  {table} → {len(df)} lignes extraites")

    # 2. Lignes de facture avec jointure (delta possible par date)
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
    df_lignes.to_sql("stg_lignes_facture", ENGINE_STG, if_exists="replace", index=False)
    log.info(f"  lignes_facture → {len(df_lignes)} lignes extraites")

    # 3. Règlements
    df_regl = pd.read_sql("SELECT * FROM reglements", ENGINE_OLTP)
    df_regl.to_sql("stg_reglements", ENGINE_STG, if_exists="replace", index=False)
    log.info(f"  reglements → {len(df_regl)} lignes extraites")


if __name__ == "__main__":
    extract_all()
