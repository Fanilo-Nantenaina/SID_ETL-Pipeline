import pandas as pd
import numpy as np
from etl_config import ENGINE_STG


def transform_clients():
    df = pd.read_sql("SELECT * FROM stg_clients WHERE stg_status='PENDING'", ENGINE_STG)
    df["nom"] = df["nom"].str.strip().str.upper()
    df["ville"] = df["ville"].str.strip().str.title().fillna("Non renseigné")
    df["region"] = df["region"].str.strip().str.title().fillna("Non renseigné")
    df["segment"] = df["segment"].fillna("Particulier")
    df["date_entree"] = pd.to_datetime(df["date_entree"], errors="coerce").fillna(
        pd.Timestamp("2020-01-01")
    )
    df["stg_status"] = "CLEAN"
    df.to_sql("stg_clients", ENGINE_STG, if_exists="replace", index=False)
    return df


def transform_lignes():
    df = pd.read_sql("SELECT * FROM stg_lignes_facture", ENGINE_STG)
    df_prod = pd.read_sql("SELECT id_produit, prix_achat FROM stg_produits", ENGINE_STG)

    df = df.merge(df_prod, on="id_produit", how="left")
    df["montant_ht"] = (
        df["quantite"] * df["prix_unitaire"] * (1 - df["remise_pct"] / 100)
    ).round(2)
    df["montant_ttc"] = (df["montant_ht"] * 1.20).round(2)
    df["remise"] = (
        df["quantite"] * df["prix_unitaire"] * df["remise_pct"] / 100
    ).round(2)
    df["marge"] = (
        df["montant_ht"] - df["quantite"] * df["prix_achat"].fillna(0)
    ).round(2)
    df["stg_status"] = "CLEAN"
    df.drop(columns=["prix_achat"], inplace=True)
    df.to_sql(
        "stg_lignes_facture",
        ENGINE_STG,
        if_exists="replace",
        index=False,
        chunksize=500,
    )
    return df
