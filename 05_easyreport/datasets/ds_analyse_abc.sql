-- ============================================================
-- 05_easyreport/datasets/ds_analyse_abc.sql
-- Rapport R6 : Analyse ABC des clients (Pareto)
-- Paramètre  : annee (INTEGER)
-- Catégories : A = top 80% du CA | B = 80-95% | C = 95-100%
-- ============================================================
WITH ca_clients AS (
  SELECT
    c.nom,
    c.segment,
    c.ville,
    c.region,
    ROUND(SUM(f.montant_ht), 0) AS ca
  FROM
    fait_ventes f
    JOIN dim_client c ON c.id_client = f.id_client
    AND c.est_actuel = TRUE
    JOIN dim_temps t ON t.id_date = f.id_date
  WHERE
    t.annee = :annee
  GROUP BY
    c.id_client,
    c.nom,
    c.segment,
    c.ville,
    c.region
),
ca_total AS (
  SELECT
    SUM(ca) AS total
  FROM
    ca_clients
),
ranked AS (
  SELECT
    nom,
    segment,
    ville,
    region,
    ca,
    ROUND(
      ca / (
        SELECT
          total
        FROM
          ca_total
      ) * 100,
      2
    ) AS part_pct,
    ROUND(
      SUM(ca) OVER (
        ORDER BY
          ca DESC ROWS UNBOUNDED PRECEDING
      ) / (
        SELECT
          total
        FROM
          ca_total
      ) * 100,
      2
    ) AS cumul_pct
  FROM
    ca_clients
)
SELECT
  nom AS client,
  segment,
  ville,
  region,
  ca,
  part_pct,
  cumul_pct,
  CASE
    WHEN cumul_pct <= 80 THEN 'A - Priorité'
    WHEN cumul_pct <= 95 THEN 'B - Développement'
    ELSE 'C - Standard'
  END AS categorie_abc
FROM
  ranked
ORDER BY
  ca DESC;
-- Paramètre à déclarer : annee (INTEGER, ex: 2024)
  -- Attendu : environ 10 clients en catégorie A (≈ 60% du CA)