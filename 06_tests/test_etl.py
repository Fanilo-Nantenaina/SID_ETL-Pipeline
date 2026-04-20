"""
06_tests/test_etl.py
Vérification automatisée des volumes et de la cohérence CA entre OLTP et Datamart.
Lancer depuis 03_etl/ : python ../06_tests/test_etl.py
"""

import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "03_etl"))

import pandas as pd
from etl_config import ENGINE_OLTP, ENGINE_STG, ENGINE_DM

PASS = "  ✅"
FAIL = "  ❌"


def test_volumes():
    print("=== TEST VOLUMES ===")

    n_oltp = pd.read_sql("SELECT COUNT(*) AS n FROM lignes_facture", ENGINE_OLTP).iloc[
        0
    ]["n"]
    n_stg = pd.read_sql(
        "SELECT COUNT(*) AS n FROM stg_lignes_facture WHERE stg_status='CLEAN'",
        ENGINE_STG,
    ).iloc[0]["n"]
    n_dm = pd.read_sql("SELECT COUNT(*) AS n FROM fait_ventes", ENGINE_DM).iloc[0]["n"]

    print(f"  OLTP  lignes_facture : {n_oltp}")
    print(f"  STG   stg_lignes (CLEAN) : {n_stg}")
    print(f"  DM    fait_ventes : {n_dm}")

    ok1 = n_stg == n_oltp
    ok2 = n_dm == n_stg
    print(
        PASS if ok1 else FAIL,
        f"Staging : {'aucune perte' if ok1 else f'{n_oltp - n_stg} lignes perdues'}",
    )
    print(
        PASS if ok2 else FAIL,
        f"Chargement DM : {'aucune perte' if ok2 else f'{n_stg - n_dm} lignes perdues'}",
    )

    assert ok1, f"Perte en staging : {n_oltp - n_stg} lignes"
    assert ok2, f"Perte au chargement DM : {n_stg - n_dm} lignes"


def test_ca_coherence():
    print("\n=== TEST CA OLTP vs DATAMART ===")

    ca_oltp = pd.read_sql(
        "SELECT ROUND(SUM(l.quantite * l.prix_unitaire * (1 - l.remise_pct / 100)), 0) AS ca "
        "FROM lignes_facture l",
        ENGINE_OLTP,
    ).iloc[0]["ca"]

    ca_dm = pd.read_sql(
        "SELECT ROUND(SUM(montant_ht), 0) AS ca FROM fait_ventes", ENGINE_DM
    ).iloc[0]["ca"]

    diff_pct = (
        abs(float(ca_oltp) - float(ca_dm)) / float(ca_oltp) * 100 if ca_oltp else 0
    )

    print(f"  CA OLTP     : {ca_oltp:>20,.0f} Ar")
    print(f"  CA Datamart : {ca_dm:>20,.0f} Ar")
    print(f"  Écart       : {diff_pct:.4f}%")

    ok = diff_pct < 0.01
    print(
        PASS if ok else FAIL,
        (
            f"CA cohérent (écart < 0.01%)"
            if ok
            else f"Écart trop important : {diff_pct:.4f}%"
        ),
    )
    assert ok, f"Écart CA : {diff_pct:.4f}%"


def test_dim_temps_coverage():
    print("\n=== TEST COUVERTURE dim_temps ===")

    orphelins = pd.read_sql(
        "SELECT COUNT(*) AS n FROM fait_ventes "
        "WHERE id_date NOT IN (SELECT id_date FROM dim_temps)",
        ENGINE_DM,
    ).iloc[0]["n"]

    ok = orphelins == 0
    print(f"  Orphelins id_date : {orphelins}")
    print(
        PASS if ok else FAIL, "Toutes les dates de fait_ventes existent dans dim_temps"
    )
    assert ok, f"{orphelins} lignes avec id_date orphelin"


def test_dim_client_coverage():
    print("\n=== TEST COUVERTURE dim_client ===")

    orphelins = pd.read_sql(
        "SELECT COUNT(*) AS n FROM fait_ventes "
        "WHERE id_client NOT IN (SELECT id_client FROM dim_client)",
        ENGINE_DM,
    ).iloc[0]["n"]

    ok = orphelins == 0
    print(f"  Orphelins id_client : {orphelins}")
    print(
        PASS if ok else FAIL, "Tous les clients de fait_ventes existent dans dim_client"
    )
    assert ok, f"{orphelins} lignes avec id_client orphelin"


if __name__ == "__main__":
    errors = []
    for fn in [
        test_volumes,
        test_ca_coherence,
        test_dim_temps_coverage,
        test_dim_client_coverage,
    ]:
        try:
            fn()
        except AssertionError as e:
            errors.append(str(e))

    print("\n" + "=" * 50)
    if errors:
        print(f"❌ {len(errors)} test(s) échoué(s) :")
        for e in errors:
            print(f"   • {e}")
        sys.exit(1)
    else:
        print("✅ Tous les tests sont passés.")
