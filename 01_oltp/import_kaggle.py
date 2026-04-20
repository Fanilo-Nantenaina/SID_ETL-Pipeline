import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("mysql+mysqlconnector://sid:SidPass2025!@localhost/ventes_oltp")
df = pd.read_csv("Sample - Superstore.csv", encoding="latin-1")

familles = df[["Category"]].drop_duplicates().reset_index(drop=True)
familles.columns = ["libelle"]
familles.to_sql("familles", engine, if_exists="replace", index=False)

clients_raw = df[
    ["Customer ID", "Customer Name", "City", "Region", "Segment"]
].drop_duplicates()
clients = pd.DataFrame(
    {
        "code_client": clients_raw["Customer ID"],
        "nom": clients_raw["Customer Name"],
        "ville": clients_raw["City"],
        "region": clients_raw["Region"],
        "segment": clients_raw["Segment"].map(
            {
                "Consumer": "Particulier",
                "Corporate": "Entreprise",
                "Home Office": "Particulier",
            }
        ),
        "date_entree": "2022-01-01",
    }
)
clients.to_sql("clients", engine, if_exists="replace", index=False)

prods = df[["Product ID", "Product Name", "Category", "Sub-Category"]].drop_duplicates()
fam_map = pd.read_sql("SELECT id_famille, libelle FROM familles", engine)
prods = prods.merge(fam_map, left_on="Category", right_on="libelle", how="left")
produits = pd.DataFrame(
    {
        "code_produit": prods["Product ID"],
        "libelle": prods["Product Name"],
        "id_famille": prods["id_famille"],
        "marque": prods["Sub-Category"],
        "prix_achat": 0,
        "unite": "unité",
    }
)
produits.to_sql("produits", engine, if_exists="replace", index=False)
print("Import Kaggle terminé")
