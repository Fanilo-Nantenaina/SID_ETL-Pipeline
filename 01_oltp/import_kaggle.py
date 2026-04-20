import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine("mysql+mysqlconnector://sid:SidPass2025!@localhost/ventes_oltp")
df = pd.read_csv("Sample - Superstore.csv", encoding="latin-1")

TRUNCATE_ORDER = [
    "reglements",
    "lignes_facture",
    "factures",
    "produits",
    "clients",
    "commerciaux",
    "modes_reglement",
    "familles",
]

with engine.begin() as conn:
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
    for table in TRUNCATE_ORDER:
        conn.execute(text(f"TRUNCATE TABLE `{table}`"))
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))

    familles = df[["Category"]].drop_duplicates().reset_index(drop=True)
    familles.columns = ["libelle"]
    familles.to_sql("familles", conn, if_exists="append", index=False)

    clients_raw = df[
        ["Customer ID", "Customer Name", "City", "Region", "Segment"]
    ].drop_duplicates(subset=["Customer ID"], keep="first")
    clients = pd.DataFrame(
        {
            "code_client": clients_raw["Customer ID"].values,
            "nom": clients_raw["Customer Name"].values,
            "ville": clients_raw["City"].values,
            "region": clients_raw["Region"].values,
            "segment": clients_raw["Segment"]
            .map(
                {
                    "Consumer": "Particulier",
                    "Corporate": "Entreprise",
                    "Home Office": "Particulier",
                }
            )
            .values,
            "date_entree": "2022-01-01",
        }
    )
    clients.to_sql("clients", conn, if_exists="append", index=False)

    prods = df[
        ["Product ID", "Product Name", "Category", "Sub-Category"]
    ].drop_duplicates(subset=["Product ID"], keep="first")

    fam_map = pd.read_sql("SELECT id_famille, libelle FROM familles", conn)
    prods = prods.merge(fam_map, left_on="Category", right_on="libelle", how="left")
    produits = pd.DataFrame(
        {
            "code_produit": prods["Product ID"].values,
            "libelle": prods["Product Name"].values,
            "id_famille": prods["id_famille"].astype("Int64").values,
            "marque": prods["Sub-Category"].values,
            "prix_achat": 0,
            "unite": "unité",
        }
    )
    produits.to_sql("produits", conn, if_exists="append", index=False)

print("Import Kaggle terminé")
