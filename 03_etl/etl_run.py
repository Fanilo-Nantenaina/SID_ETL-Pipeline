import logging, sys, time
from etl_extract import extract_all
from etl_transform import transform_clients, transform_lignes
from etl_load import load_dim_temps, load_dim_client, load_dim_simple, load_fait_ventes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/etl_run.log"),
    ],
)
log = logging.getLogger()


def run():
    t0 = time.time()
    log.info("=== ETL DÉMARRÉ ===")

    log.info("--- PHASE E : Extraction ---")
    extract_all()

    log.info("--- PHASE T : Transformation ---")
    transform_clients()
    transform_lignes()

    log.info("--- PHASE L : Chargement ---")
    load_dim_temps()
    load_dim_client()
    load_dim_simple(
        "stg_produits", "dim_produit", {"code_produit": "code", "libelle": "libelle"}
    )
    load_dim_simple(
        "stg_commerciaux",
        "dim_commercial",
        {"nom": "nom", "equipe": "equipe", "date_embauche": "date_embauche"},
    )
    load_dim_simple("stg_modes_reglement", "dim_mode_reglement", {"libelle": "libelle"})
    load_fait_ventes()

    log.info(f"=== ETL TERMINÉ en {time.time()-t0:.1f}s ===")


if __name__ == "__main__":
    run()
