"""
06_tests/test_etl.py
Assertions automatisées : volumes + cohérence CA OLTP ↔ Datamart
Exécuter depuis le dossier 03_etl/ :
    cd ../06_tests && python3 test_etl.py
ou directement :
    python3 06_tests/test_etl.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "03_etl"))

import pandas as pd
from etl_config import ENGINE_OLTP, ENGINE_STG, ENGINE_DM


# ──────────────────────────────────────────────────────────────
def header(title):
    print(f"\n{'='*55}")
    print(f"  {title}")
    print(f"{'='*55}")


def ok(msg):
    print(f"  ✅  {msg}")


def fail(msg):
    print(f"  ❌  {msg}")
    raise AssertionError(msg)


# ──────────────────────────────────────────────────────────────
def test_volumes_oltp():
    header("TEST 1 — Volumes OLTP")
    expected = {
        "familles": 10,
        "produits": 150,
        "clients": 60,
        "commerciaux": 4,
        "modes_reglement": 4,
    }
    for table, minimum in expected.items():
        n = pd.read_sql(f"SELECT COUNT(*) AS n FROM {table}", ENGINE_OLTP).iloc[0]["n"]
        print(f"  {table:<20} : {n}")
        if n < minimum:
            fail(f"{table} : attendu >= {minimum}, obtenu {n}")
    ok("Volumes OLTP cohérents")

    # Factures
    n_fac = pd.read_sql("SELECT COUNT(*) AS n FROM factures", ENGINE_OLTP).iloc[0]["n"]
    n_lig = pd.read_sql("SELECT COUNT(*) AS n FROM lignes_facture", ENGINE_OLTP).iloc[
        0
    ]["n"]
    print(f"  {'factures':<20} : {n_fac}")
    print(f"  {'lignes_facture':<20} : {n_lig}")
    if n_fac < 2900:
        fail(f"Factures : attendu >= 2900, obtenu {n_fac}")
    if n_lig < 7000:
        fail(f"Lignes : attendu >= 7000, obtenu {n_lig}")
    ok("Factures et lignes cohérents")


# ──────────────────────────────────────────────────────────────
def test_staging_completeness():
    header("TEST 2 — Staging : lignes CLEAN")
    n_oltp = pd.read_sql(
        "SELECT COUNT(*) AS n FROM ventes_oltp.lignes_facture", ENGINE_OLTP
    ).iloc[0]["n"]
    n_stg = pd.read_sql(
        "SELECT COUNT(*) AS n FROM stg_lignes_facture WHERE stg_status='CLEAN'",
        ENGINE_STG,
    ).iloc[0]["n"]
    print(f"  Lignes OLTP    : {n_oltp}")
    print(f"  Lignes CLEAN   : {n_stg}")
    if n_stg < n_oltp * 0.99:
        fail(f"Perte en staging > 1% : {n_oltp - n_stg} lignes perdues")
    ok("Staging complet (< 1% de perte)")

    n_cli_clean = pd.read_sql(
        "SELECT COUNT(*) AS n FROM stg_clients WHERE stg_status='CLEAN'",
        ENGINE_STG,
    ).iloc[0]["n"]
    print(f"  Clients CLEAN  : {n_cli_clean}")
    if n_cli_clean < 55:
        fail(f"Trop peu de clients CLEAN : {n_cli_clean}")
    ok("Clients staging OK")


# ──────────────────────────────────────────────────────────────
def test_datamart_volumes():
    header("TEST 3 — Volumes Datamart")
    tables_dm = {
        "dim_temps": 1826,
        "dim_client": 55,
        "dim_produit": 140,
        "dim_commercial": 4,
        "dim_mode_reglement": 4,
    }
    for table, minimum in tables_dm.items():
        n = pd.read_sql(f"SELECT COUNT(*) AS n FROM {table}", ENGINE_DM).iloc[0]["n"]
        print(f"  {table:<25} : {n}")
        if n < minimum:
            fail(f"{table} : attendu >= {minimum}, obtenu {n}")

    n_fv = pd.read_sql("SELECT COUNT(*) AS n FROM fait_ventes", ENGINE_DM).iloc[0]["n"]
    n_fr = pd.read_sql("SELECT COUNT(*) AS n FROM fait_reglements", ENGINE_DM).iloc[0][
        "n"
    ]
    print(f"  {'fait_ventes':<25} : {n_fv}")
    print(f"  {'fait_reglements':<25} : {n_fr}")
    if n_fv < 7000:
        fail(f"fait_ventes : attendu >= 7000, obtenu {n_fv}")
    if n_fr < 2000:
        fail(f"fait_reglements : attendu >= 2000, obtenu {n_fr}")
    ok("Volumes datamart cohérents")


# ──────────────────────────────────────────────────────────────
def test_ca_coherence():
    header("TEST 4 — CA OLTP vs Datamart")
    ca_oltp = pd.read_sql(
        """SELECT ROUND(SUM(l.quantite * l.prix_unitaire * (1 - l.remise_pct / 100)), 0) AS ca
           FROM ventes_oltp.lignes_facture l""",
        ENGINE_OLTP,
    ).iloc[0]["ca"]

    ca_dm = pd.read_sql(
        "SELECT ROUND(SUM(montant_ht), 0) AS ca FROM fait_ventes",
        ENGINE_DM,
    ).iloc[0]["ca"]

    diff_pct = abs(ca_oltp - ca_dm) / ca_oltp * 100
    print(f"  CA OLTP      : {ca_oltp:>20,.0f} Ar")
    print(f"  CA Datamart  : {ca_dm:>20,.0f} Ar")
    print(f"  Écart        : {diff_pct:.4f}%")
    if diff_pct >= 0.01:
        fail(f"Écart CA trop important : {diff_pct:.4f}%")
    ok("CA cohérent (écart < 0.01%)")


# ──────────────────────────────────────────────────────────────
def test_orphelins():
    header("TEST 5 — Clés orphelines dans fait_ventes")
    checks = [
        (
            "id_date orphelin",
            "SELECT COUNT(*) AS n FROM fait_ventes WHERE id_date NOT IN (SELECT id_date FROM dim_temps)",
        ),
        (
            "id_client orphelin",
            "SELECT COUNT(*) AS n FROM fait_ventes WHERE id_client NOT IN (SELECT id_client FROM dim_client)",
        ),
        (
            "id_produit orphelin",
            "SELECT COUNT(*) AS n FROM fait_ventes WHERE id_produit NOT IN (SELECT id_produit FROM dim_produit)",
        ),
        (
            "id_commercial orphelin",
            "SELECT COUNT(*) AS n FROM fait_ventes WHERE id_commercial NOT IN (SELECT id_commercial FROM dim_commercial)",
        ),
    ]
    for label, sql in checks:
        n = pd.read_sql(sql, ENGINE_DM).iloc[0]["n"]
        print(f"  {label:<25} : {n}")
        if n > 0:
            fail(f"{label} : {n} orphelins détectés")
    ok("Aucune clé orpheline dans fait_ventes")


# ──────────────────────────────────────────────────────────────
def test_saisonnalite():
    header("TEST 6 — Saisonnalité (T4 doit être le plus élevé)")
    df = pd.read_sql(
        """SELECT t.trimestre, ROUND(SUM(f.montant_ht),0) AS ca_ht
           FROM fait_ventes f
           JOIN dim_temps t ON t.id_date = f.id_date
           GROUP BY t.trimestre
           ORDER BY t.trimestre""",
        ENGINE_DM,
    )
    for _, row in df.iterrows():
        print(f"  T{int(row['trimestre'])} : {row['ca_ht']:>20,.0f} Ar")

    t4_ca = df[df["trimestre"] == 4]["ca_ht"].values[0]
    max_ca = df["ca_ht"].max()
    if t4_ca != max_ca:
        fail(
            f"T4 n'est pas le trimestre le plus élevé (T4={t4_ca:,.0f} vs max={max_ca:,.0f})"
        )
    ok("Saisonnalité correcte : T4 est le plus fort")


# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    errors = []
    for fn in [
        test_volumes_oltp,
        test_staging_completeness,
        test_datamart_volumes,
        test_ca_coherence,
        test_orphelins,
        test_saisonnalite,
    ]:
        try:
            fn()
        except AssertionError as e:
            errors.append(str(e))

    print(f"\n{'='*55}")
    if errors:
        print(f"  ❌  {len(errors)} test(s) échoué(s) :")
        for e in errors:
            print(f"      • {e}")
        sys.exit(1)
    else:
        print("  ✅  Tous les tests passés avec succès.")
    print(f"{'='*55}\n")
