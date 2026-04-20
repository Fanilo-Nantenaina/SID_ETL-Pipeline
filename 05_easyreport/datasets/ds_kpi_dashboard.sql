-- ============================================================
-- 05_easyreport/datasets/ds_kpi_dashboard.sql
-- Rapport R5 : Tableau de bord KPI direction
-- Paramètre  : annee (INTEGER)
-- Retourne une ligne par indicateur (format indicateur/valeur)
-- ============================================================
SELECT
  'CA HT total' AS indicateur,
  CAST(ROUND(SUM(f.montant_ht), 0) AS CHAR) AS valeur,
  'Ariary' AS unite
FROM
  fait_ventes f
  JOIN dim_temps t ON t.id_date = f.id_date
WHERE
  t.annee = :annee
UNION ALL
SELECT
  'Marge totale',
  CAST(ROUND(SUM(f.marge), 0) AS CHAR),
  'Ariary'
FROM
  fait_ventes f
  JOIN dim_temps t ON t.id_date = f.id_date
WHERE
  t.annee = :annee
UNION ALL
SELECT
  'Taux de marge',
  CAST(
    ROUND(
      SUM(f.marge) / NULLIF(SUM(f.montant_ht), 0) * 100,
      1
    ) AS CHAR
  ),
  '%'
FROM
  fait_ventes f
  JOIN dim_temps t ON t.id_date = f.id_date
WHERE
  t.annee = :annee
UNION ALL
SELECT
  'Nb lignes de vente',
  CAST(COUNT(*) AS CHAR),
  'lignes'
FROM
  fait_ventes f
  JOIN dim_temps t ON t.id_date = f.id_date
WHERE
  t.annee = :annee
UNION ALL
SELECT
  'Panier moyen HT',
  CAST(ROUND(AVG(f.montant_ht), 0) AS CHAR),
  'Ariary'
FROM
  fait_ventes f
  JOIN dim_temps t ON t.id_date = f.id_date
WHERE
  t.annee = :annee
UNION ALL
SELECT
  'Nb clients actifs',
  CAST(COUNT(DISTINCT f.id_client) AS CHAR),
  'clients'
FROM
  fait_ventes f
  JOIN dim_temps t ON t.id_date = f.id_date
WHERE
  t.annee = :annee
UNION ALL
SELECT
  'Total réglé',
  CAST(ROUND(SUM(fr.montant_regle), 0) AS CHAR),
  'Ariary'
FROM
  fait_reglements fr
  JOIN dim_temps t ON t.id_date = fr.id_date
WHERE
  t.annee = :annee
UNION ALL
SELECT
  'Remises accordées',
  CAST(ROUND(SUM(f.remise), 0) AS CHAR),
  'Ariary'
FROM
  fait_ventes f
  JOIN dim_temps t ON t.id_date = f.id_date
WHERE
  t.annee = :annee;
-- Paramètre à déclarer : annee (INTEGER, ex: 2024)