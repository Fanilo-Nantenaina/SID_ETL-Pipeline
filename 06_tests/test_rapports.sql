-- 06_tests/test_rapports.sql
-- Exécuter ces requêtes directement sur ventes_dm ET comparer avec
-- les exports EasyReport correspondants (mêmes paramètres).
USE ventes_dm;
-- ─── Test R1 : CA 2024 par famille ───────────────────────────────
-- Comparer avec rapport R1 (annee_debut=2024, annee_fin=2024)
SELECT
  p.famille,
  ROUND(SUM(f.montant_ht), 0) AS ca_ht
FROM
  fait_ventes f
  JOIN dim_produit p ON p.id_produit = f.id_produit
  JOIN dim_temps t ON t.id_date = f.id_date
WHERE
  t.annee = 2024
GROUP BY
  p.famille
ORDER BY
  ca_ht DESC;
-- ─── Test R2 : Top 3 clients 2024 ────────────────────────────────
  -- Comparer avec rapport R2 (annee=2024), les 3 premières lignes
SELECT
  c.nom,
  ROUND(SUM(f.montant_ht), 0) AS ca
FROM
  fait_ventes f
  JOIN dim_client c ON c.id_client = f.id_client
  AND c.est_actuel = TRUE
  JOIN dim_temps t ON t.id_date = f.id_date
WHERE
  t.annee = 2024
GROUP BY
  c.id_client,
  c.nom
ORDER BY
  ca DESC
LIMIT
  3;
-- ─── Test Pareto : les 10 premiers clients ≈ 60% du CA ───────────
  WITH top10 AS (
    SELECT
      SUM(f.montant_ht) AS ca_top
    FROM
      fait_ventes f
      JOIN dim_client c ON c.id_client = f.id_client
      AND c.est_actuel = TRUE
      JOIN dim_temps t ON t.id_date = f.id_date
    WHERE
      t.annee = 2024
    GROUP BY
      c.id_client
    ORDER BY
      ca_top DESC
    LIMIT
      10
  )
SELECT
  ROUND(
    SUM(ca_top) / (
      SELECT
        SUM(montant_ht)
      FROM
        fait_ventes f
        JOIN dim_temps t ON t.id_date = f.id_date
      WHERE
        t.annee = 2024
    ) * 100,
    1
  ) AS part_top10_pct
FROM
  top10;
-- Attendu : ~60% si les données simulées ont la distribution Pareto
  -- ─── Test R4 : Cohérence évolution N/N-1 ─────────────────────────
  -- CA 2025 vs CA 2024 calculé mois par mois
SELECT
  t.mois,
  ROUND(
    SUM(
      CASE
        WHEN t.annee = 2025 THEN f.montant_ht
        ELSE 0
      END
    ),
    0
  ) AS ca_2025,
  ROUND(
    SUM(
      CASE
        WHEN t.annee = 2024 THEN f.montant_ht
        ELSE 0
      END
    ),
    0
  ) AS ca_2024
FROM
  fait_ventes f
  JOIN dim_temps t ON t.id_date = f.id_date
WHERE
  t.annee IN (2024, 2025)
GROUP BY
  t.mois
ORDER BY
  t.mois;
-- ─── Test reglements : taux de recouvrement global ───────────────
SELECT
  ROUND(SUM(fr.montant_regle), 0) AS total_regle,
  ROUND(SUM(fv.montant_ttc), 0) AS total_facture,
  ROUND(
    SUM(fr.montant_regle) / NULLIF(SUM(fv.montant_ttc), 0) * 100,
    1
  ) AS taux_recouvrement_pct
FROM
  fait_ventes fv
  JOIN dim_temps t ON t.id_date = fv.id_date
  LEFT JOIN fait_reglements fr ON fr.id_client = fv.id_client
  AND fr.id_commercial = fv.id_commercial
WHERE
  t.annee = 2024;
-- Attendu : ~80% (80% des factures réglées dans les données simulées)