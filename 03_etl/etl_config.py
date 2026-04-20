from sqlalchemy import create_engine

DB_USER = "sid"
DB_PASS = "SidPass2025!"
DB_HOST = "localhost"


def get_engine(db_name):
    return create_engine(
        f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{db_name}",
        pool_recycle=3600,
    )


ENGINE_OLTP = get_engine("ventes_oltp")
ENGINE_STG = get_engine("ventes_stg")
ENGINE_DM = get_engine("ventes_dm")

LOG_FILE = "logs/etl_run.log"
