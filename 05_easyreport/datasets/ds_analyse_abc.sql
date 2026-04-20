-- Dataset R6 : Analyse ABC des clients (Pareto)
-- Paramètre : annee (INTEGER)
WITH ca_clients AS (
  SELECT
    c.nom,
    c.segment,
    c.ville,
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
    c.ville
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
  nom,
  segment,
  ville,
  ca,
  part_pct,
  cumul_pct,
  CASE
    WHEN cumul_pct <= 80 THEN 'A — Priorité'
    WHEN cumul_pct <= 95 THEN 'B — Développement'
    ELSE 'C — Standard'
  END AS categorie_abc
FROM
  ranked
ORDER BY
  ca DESC;