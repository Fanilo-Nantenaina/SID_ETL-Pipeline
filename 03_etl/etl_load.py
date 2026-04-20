import pandas as pd
from datetime import date
from sqlalchemy import inspect as sql_inspect, text
from etl_config import ENGINE_STG, ENGINE_DM
import logging

log = logging.getLogger(__name__)

TODAY = date.today().isoformat()


def load_dim_temps():
    insp = sql_inspect(ENGINE_DM)
    if insp.has_table("dim_temps"):
        existing = pd.read_sql("SELECT COUNT(*) AS n FROM dim_temps", ENGINE_DM).iloc[
            0
        ]["n"]
        if existing > 0:
            log.info("dim_temps déjà peuplé — skip")
            return

    rows = []
    d = date(2022, 1, 1)
    end = date(2026, 12, 31)
    while d <= end:
        rows.append(
            {
                "id_date": int(d.strftime("%Y%m%d")),
                "jour": d.day,
                "mois": d.month,
                "trimestre": (d.month - 1) // 3 + 1,
                "semestre": 1 if d.month <= 6 else 2,
                "annee": d.year,
                "nom_mois": d.strftime("%B"),
                "jour_semaine": d.isoweekday(),
                "est_jour_ouvre": d.isoweekday() <= 5,
            }
        )
        d = date.fromordinal(d.toordinal() + 1)

    pd.DataFrame(rows).to_sql("dim_temps", ENGINE_DM, if_exists="append", index=False)
    log.info(f"dim_temps : {len(rows)} jours insérés")


def load_dim_client():
    df_stg = pd.read_sql(
        "SELECT * FROM stg_clients WHERE stg_status='CLEAN'", ENGINE_STG
    )

    insp = sql_inspect(ENGINE_DM)
    if insp.has_table("dim_client"):
        df_dm = pd.read_sql("SELECT * FROM dim_client WHERE est_actuel=TRUE", ENGINE_DM)
    else:
        df_dm = pd.DataFrame()

    new_rows, update_ids = [], []

    for _, row in df_stg.iterrows():
        match = (
            df_dm[df_dm["code"] == row["code_client"]]
            if not df_dm.empty
            else pd.DataFrame()
        )

        if match.empty:
            new_rows.append(_build_dim_row(row, TODAY))
        else:
            dm_row = match.iloc[0]
            changed = (
                row["segment"] != dm_row["segment"] or row["ville"] != dm_row["ville"]
            )
            if changed:
                update_ids.append(int(dm_row["id_client"]))
                new_rows.append(_build_dim_row(row, TODAY))

    if update_ids:
        ids_str = ",".join(map(str, update_ids))
        with ENGINE_DM.begin() as conn:
            conn.execute(
                text(
                    f"UPDATE dim_client SET date_fin='{TODAY}', est_actuel=FALSE WHERE id_client IN ({ids_str})"
                )
            )

    if new_rows:
        pd.DataFrame(new_rows).to_sql(
            "dim_client", ENGINE_DM, if_exists="append", index=False
        )
    log.info(f"dim_client : {len(new_rows)} lignes insérées, {len(update_ids)} fermées")


def _build_dim_row(row, date_debut):
    return {
        "code": row["code_client"],
        "nom": row["nom"],
        "ville": row["ville"],
        "region": row["region"],
        "segment": row["segment"],
        "date_entree": row["date_entree"],
        "date_debut": date_debut,
        "date_fin": None,
        "est_actuel": True,
    }


def load_dim_simple(stg_table, dm_table, col_map):
    df = pd.read_sql(f"SELECT * FROM {stg_table}", ENGINE_STG)
    df = df.rename(columns=col_map)
    df[list(col_map.values())].to_sql(
        dm_table, ENGINE_DM, if_exists="replace", index=False
    )
    log.info(f"{dm_table} : {len(df)} lignes chargées")


def load_fait_ventes():
    df = pd.read_sql(
        "SELECT * FROM stg_lignes_facture WHERE stg_status='CLEAN'", ENGINE_STG
    )

    dim_t = pd.read_sql("SELECT id_date FROM dim_temps", ENGINE_DM)
    dim_cli = pd.read_sql(
        "SELECT id_client, code FROM dim_client WHERE est_actuel=TRUE", ENGINE_DM
    )
    dim_pro = pd.read_sql("SELECT id_produit, code FROM dim_produit", ENGINE_DM)

    df["date_facture"] = pd.to_datetime(df["date_facture"])
    df["id_date_key"] = df["date_facture"].dt.strftime("%Y%m%d").astype(int)
    df = df.merge(
        dim_t[["id_date"]], left_on="id_date_key", right_on="id_date", how="left"
    )

    df_cli_map = pd.read_sql(
        "SELECT id_client AS id_stg, code_client FROM stg_clients", ENGINE_STG
    )
    df_cli_map = df_cli_map.merge(
        dim_cli, left_on="code_client", right_on="code", how="left"
    )
    df = df.merge(
        df_cli_map[["id_stg", "id_client_y"]],
        left_on="id_client",
        right_on="id_stg",
        how="left",
    )
    df.rename(columns={"id_client_y": "fk_client"}, inplace=True)

    fait = pd.DataFrame(
        {
            "id_date": df["id_date"],
            "id_client": df["fk_client"],
            "id_produit": df["id_produit"],
            "id_commercial": df["id_commercial"],
            "quantite": df["quantite"],
            "montant_ht": df["montant_ht"],
            "montant_ttc": df["montant_ttc"],
            "marge": df["marge"],
            "remise": df["remise"],
        }
    )

    fait.to_sql(
        "fait_ventes", ENGINE_DM, if_exists="replace", index=False, chunksize=500
    )
    log.info(f"fait_ventes : {len(fait)} lignes chargées")
