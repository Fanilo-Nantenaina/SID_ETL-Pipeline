USE ventes_dm;
-- ① Volumes des tables
SELECT
  'dim_temps' AS table_dm,
  COUNT(*) AS nb
FROM
  dim_temps
UNION ALL
SELECT
  'dim_client',
  COUNT(*)
FROM
  dim_client
UNION ALL
SELECT
  'dim_produit',
  COUNT(*)
FROM
  dim_produit
UNION ALL
SELECT
  'dim_commercial',
  COUNT(*)
FROM
  dim_commercial
UNION ALL
SELECT
  'dim_mode',
  COUNT(*)
FROM
  dim_mode_reglement
UNION ALL
SELECT
  'fait_ventes',
  COUNT(*)
FROM
  fait_ventes
UNION ALL
SELECT
  'fait_reglements',
  COUNT(*)
FROM
  fait_reglements;
-- Attendu : ~1826 / 60 / 150 / 4 / 4 / ~8000 / ~2400
  -- ② Clés étrangères orphelines dans fait_ventes (doit retourner 0)
SELECT
  COUNT(*) AS orphelins_date
FROM
  fait_ventes
WHERE
  id_date NOT IN (
    SELECT
      id_date
    FROM
      dim_temps
  );
SELECT
  COUNT(*) AS orphelins_client
FROM
  fait_ventes
WHERE
  id_client NOT IN (
    SELECT
      id_client
    FROM
      dim_client
  );
-- ③ Contrôle croisé OLTP ↔ Datamart (le CA total doit être identique)
SELECT
  'OLTP' AS source,
  ROUND(
    SUM(
      l.quantite * l.prix_unitaire * (1 - l.remise_pct / 100)
    ),
    0
  ) AS ca_ht
FROM
  ventes_oltp.lignes_facture l
UNION ALL
SELECT
  'Datamart',
  ROUND(SUM(montant_ht), 0)
FROM
  fait_ventes;
-- ④ CA par année dans le datamart
SELECT
  t.annee,
  ROUND(SUM(f.montant_ht), 0) AS ca_ht,
  COUNT(*) AS nb_lignes
FROM
  fait_ventes f
  JOIN dim_temps t ON t.id_date = f.id_date
GROUP BY
  t.annee
ORDER BY
  t.annee;
-- ⑤ Vérification de la saisonnalité (T4 doit être le plus élevé)
SELECT
  t.trimestre,
  ROUND(SUM(f.montant_ht), 0) AS ca_ht
FROM
  fait_ventes f
  JOIN dim_temps t ON t.id_date = f.id_date
GROUP BY
  t.trimestre
ORDER BY
  t.trimestre;