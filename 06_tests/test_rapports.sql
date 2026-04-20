-- ============================================================
-- 06_tests/test_rapports.sql
-- Requêtes de vérification croisée SQL ↔ exports EasyReport
-- Exécuter dans MySQL Workbench / phpMyAdmin sur ventes_dm
-- Comparer les résultats avec les fichiers exportés depuis EasyReport
-- ============================================================
USE ventes_dm;
-- ─────────────────────────────────────────────────────────────
-- TEST R1 — CA 2024 par famille
-- Comparer avec l'export du rapport R1 (paramètres 2024-2024)
-- ─────────────────────────────────────────────────────────────
SELECT
  p.famille,
  ROUND(SUM(f.montant_ht), 0) AS ca_ht,
  ROUND(SUM(f.marge), 0) AS marge,
  ROUND(SUM(f.quantite), 1) AS quantite
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
-- ─────────────────────────────────────────────────────────────
  -- TEST R2 — Top 3 clients 2024
  -- Comparer avec l'export du rapport R2 (paramètre 2024)
  -- ─────────────────────────────────────────────────────────────
SELECT
  c.nom,
  c.segment,
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
  c.nom,
  c.segment
ORDER BY
  ca DESC
LIMIT
  3;
-- ─────────────────────────────────────────────────────────────
  -- TEST Pareto — les 10 premiers clients = X% du CA 2024
  -- Attendu : environ 60% (distribution Pareto du générateur)
  -- ─────────────────────────────────────────────────────────────
  WITH top10 AS (
    SELECT
      f.id_client,
      SUM(f.montant_ht) AS ca_top
    FROM
      fait_ventes f
      JOIN dim_temps t ON t.id_date = f.id_date
    WHERE
      t.annee = 2024
    GROUP BY
      f.id_client
    ORDER BY
      ca_top DESC
    LIMIT
      10
  )
SELECT
  ROUND(
    SUM(ca_top) / (
      SELECT
        SUM(fv2.montant_ht)
      FROM
        fait_ventes fv2
        JOIN dim_temps tv ON tv.id_date = fv2.id_date
      WHERE
        tv.annee = 2024
    ) * 100,
    1
  ) AS part_top10_pct
FROM
  top10;
-- Attendu : environ 60%
  -- ─────────────────────────────────────────────────────────────
  -- TEST R4 — Évolution N vs N-1 (2025 vs 2024)
  -- Comparer avec l'export du rapport R4 (paramètre 2025)
  -- ─────────────────────────────────────────────────────────────
SELECT
  t.mois,
  t.nom_mois,
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
  ) AS ca_2024,
  ROUND(
    (
      SUM(
        CASE
          WHEN t.annee = 2025 THEN f.montant_ht
          ELSE 0
        END
      ) - SUM(
        CASE
          WHEN t.annee = 2024 THEN f.montant_ht
          ELSE 0
        END
      )
    ) / NULLIF(
      SUM(
        CASE
          WHEN t.annee = 2024 THEN f.montant_ht
          ELSE 0
        END
      ),
      0
    ) * 100,
    1
  ) AS evolution_pct
FROM
  fait_ventes f
  JOIN dim_temps t ON t.id_date = f.id_date
WHERE
  t.annee IN (2024, 2025)
GROUP BY
  t.mois,
  t.nom_mois
ORDER BY
  t.mois;
-- ─────────────────────────────────────────────────────────────
  -- TEST R6 — Analyse ABC 2024
  -- Comparer avec l'export du rapport R6 (paramètre 2024)
  -- ─────────────────────────────────────────────────────────────
SELECT
  categorie_abc,
  COUNT(*) AS nb_clients,
  ROUND(SUM(ca), 0) AS ca_total,
  ROUND(AVG(part_pct), 2) AS part_moyenne_pct
FROM
  (
    WITH ca_cli AS (
      SELECT
        c.id_client,
        c.nom,
        c.segment,
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
        c.nom,
        c.segment
    ),
    total AS (
      SELECT
        SUM(ca) AS tot
      FROM
        ca_cli
    ),
    rnk AS (
      SELECT
        nom,
        segment,
        ca,
        ROUND(
          ca / (
            SELECT
              tot
            FROM
              total
          ) * 100,
          2
        ) AS part_pct,
        ROUND(
          SUM(ca) OVER (
            ORDER BY
              ca DESC ROWS UNBOUNDED PRECEDING
          ) / (
            SELECT
              tot
            FROM
              total
          ) * 100,
          2
        ) AS cumul_pct
      FROM
        ca_cli
    )
    SELECT
      nom,
      segment,
      ca,
      part_pct,
      cumul_pct,
      CASE
        WHEN cumul_pct <= 80 THEN 'A - Priorité'
        WHEN cumul_pct <= 95 THEN 'B - Développement'
        ELSE 'C - Standard'
      END AS categorie_abc
    FROM
      rnk
  ) ranked
GROUP BY
  categorie_abc
ORDER BY
  categorie_abc;
-- ─────────────────────────────────────────────────────────────
  -- TEST Recouvrement — taux global 2024
  -- ─────────────────────────────────────────────────────────────
SELECT
  ROUND(SUM(fv.montant_ttc), 0) AS total_facture_ttc,
  ROUND(COALESCE(SUM(fr.montant_regle), 0), 0) AS total_regle,
  ROUND(
    COALESCE(SUM(fr.montant_regle), 0) / NULLIF(SUM(fv.montant_ttc), 0) * 100,
    1
  ) AS taux_recouvrement_pct
FROM
  fait_ventes fv
  JOIN dim_temps t ON t.id_date = fv.id_date
  LEFT JOIN fait_reglements fr ON fr.id_client = fv.id_client
  AND fr.id_commercial = fv.id_commercial
WHERE
  t.annee = 2024;
-- Attendu : environ 80%