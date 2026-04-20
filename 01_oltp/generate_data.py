import random, string
from datetime import date, timedelta
from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("mysql+mysqlconnector://sid:SidPass2025!@localhost/ventes_oltp")

random.seed(42)

FAMILLES = [
    "Informatique",
    "Mobilier",
    "Fournitures",
    "Impression",
    "Télécommunication",
    "Électronique",
    "Papeterie",
    "Hygiène",
    "Emballage",
    "Sécurité",
]
df_fam = pd.DataFrame({"libelle": FAMILLES})
df_fam.to_sql("familles", engine, if_exists="append", index=False)
print(f"{len(df_fam)} familles insérées")

MARQUES = ["Samsung", "HP", "Canon", "Epson", "Dell", "Lenovo", "Generic"]
produits = []
for fam_id in range(1, 11):
    for j in range(15):
        px = round(random.uniform(5000, 500000), -2)
        produits.append(
            {
                "code_produit": f"P{fam_id:02}{j:02}",
                "libelle": f"Produit {fam_id}-{j+1:02}",
                "id_famille": fam_id,
                "marque": random.choice(MARQUES),
                "prix_achat": px,
                "unite": "unité",
            }
        )
pd.DataFrame(produits).to_sql("produits", engine, if_exists="append", index=False)
print(f"{len(produits)} produits insérés")

VILLES = [
    "Antananarivo",
    "Fianarantsoa",
    "Toamasina",
    "Mahajanga",
    "Toliary",
    "Antsirabe",
]
REGIONS = [
    "Analamanga",
    "Haute Matsiatra",
    "Atsinanana",
    "Boeny",
    "Atsimo-Andrefana",
    "Vakinankaratra",
]
SEGMENTS = ["Entreprise", "Administration", "Particulier"]
SEG_W = [60, 20, 20]
clients = []
start_d = date(2022, 1, 1)
for i in range(1, 61):
    idx = random.randint(0, 5)
    clients.append(
        {
            "code_client": f"C{i:04}",
            "nom": f"Client {i:02}",
            "ville": VILLES[idx],
            "region": REGIONS[idx],
            "segment": random.choices(SEGMENTS, weights=SEG_W)[0],
            "date_entree": (
                start_d + timedelta(days=random.randint(0, 365))
            ).isoformat(),
        }
    )
pd.DataFrame(clients).to_sql("clients", engine, if_exists="append", index=False)

COMM = [
    {"nom": "Rakoto Jean", "equipe": "Nord", "date_embauche": "2020-03-01"},
    {"nom": "Rabe Marie", "equipe": "Nord", "date_embauche": "2019-06-15"},
    {"nom": "Rasoa Pierre", "equipe": "Sud", "date_embauche": "2021-01-10"},
    {"nom": "Randria Luc", "equipe": "Sud", "date_embauche": "2022-04-01"},
]
pd.DataFrame(COMM).to_sql("commerciaux", engine, if_exists="append", index=False)

MODES = [
    {"libelle": "Virement"},
    {"libelle": "Chèque"},
    {"libelle": "Espèces"},
    {"libelle": "Mobile Money"},
]
pd.DataFrame(MODES).to_sql("modes_reglement", engine, if_exists="append", index=False)


def random_date(year):
    month = random.choices(
        list(range(1, 13)),
        weights=[5, 5, 5, 5, 5, 5, 5, 5, 8, 10, 12, 20],
    )[0]
    day = random.randint(1, 28)
    return date(year, month, day)


factures_rows = []
lignes_rows = []
reglements_rows = []
fact_id = 1
ligne_id = 1
regl_id = 1

for year in [2023, 2024, 2025]:
    n_factures = 1000
    for _ in range(n_factures):
        cli_id = random.choices(list(range(1, 61)), weights=[6] * 10 + [1] * 50)[0]
        com_id = random.randint(1, 4)
        d = random_date(year)
        num = f"F{year}{fact_id:05}"

        factures_rows.append(
            {
                "numero": num,
                "date_facture": d.isoformat(),
                "id_client": cli_id,
                "id_commercial": com_id,
            }
        )

        n_lignes = random.randint(1, 6)
        produit_ids = random.sample(range(1, 151), n_lignes)
        total_ttc = 0
        for pid in produit_ids:
            qty = round(random.uniform(1, 20), 1)
            prix = round(random.uniform(10000, 300000), -2)
            rem = random.choices([0, 5, 10, 15], [70, 15, 10, 5])[0]
            ht = qty * prix * (1 - rem / 100)
            total_ttc += ht * 1.20
            lignes_rows.append(
                {
                    "id_facture": fact_id,
                    "id_produit": pid,
                    "quantite": qty,
                    "prix_unitaire": prix,
                    "remise_pct": rem,
                }
            )
            ligne_id += 1

        if random.random() < 0.80:
            days_later = random.randint(0, 90)
            d_regl = d + timedelta(days=days_later)
            reglements_rows.append(
                {
                    "id_facture": fact_id,
                    "id_commercial": com_id,
                    "id_mode": random.randint(1, 4),
                    "date_reglement": d_regl.isoformat(),
                    "montant": round(total_ttc, 2),
                }
            )
            regl_id += 1

        fact_id += 1

pd.DataFrame(factures_rows).to_sql(
    "factures", engine, if_exists="append", index=False, chunksize=500
)
pd.DataFrame(lignes_rows).to_sql(
    "lignes_facture", engine, if_exists="append", index=False, chunksize=500
)
pd.DataFrame(reglements_rows).to_sql(
    "reglements", engine, if_exists="append", index=False, chunksize=500
)
print(
    f"{len(factures_rows)} factures, {len(lignes_rows)} lignes, {len(reglements_rows)} règlements"
)
