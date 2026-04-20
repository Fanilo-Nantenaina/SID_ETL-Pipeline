-- ============================================================
-- 05_easyreport/datasets/ds_evolution_nn1.sql
-- Rapport R4 : Évolution CA N vs N-1 par mois
-- Paramètre  : annee (INTEGER) — l'année N
-- ============================================================
SELECT
  t.mois,
  t.nom_mois,
  ROUND(
    SUM(
      CASE
        WHEN t.annee = :annee THEN f.montant_ht
        ELSE 0
      END
    ),
    0
  ) AS ca_n,
  ROUND(
    SUM(
      CASE
        WHEN t.annee = :annee - 1 THEN f.montant_ht
        ELSE 0
      END
    ),
    0
  ) AS ca_n1,
  ROUND(
    (
      SUM(
        CASE
          WHEN t.annee = :annee THEN f.montant_ht
          ELSE 0
        END
      ) - SUM(
        CASE
          WHEN t.annee = :annee - 1 THEN f.montant_ht
          ELSE 0
        END
      )
    ) / NULLIF(
      SUM(
        CASE
          WHEN t.annee = :annee - 1 THEN f.montant_ht
          ELSE 0
        END
      ),
      0
    ) * 100,
    1
  ) AS evolution_pct,
  ROUND(
    SUM(
      CASE
        WHEN t.annee = :annee THEN f.marge
        ELSE 0
      END
    ),
    0
  ) AS marge_n,
  ROUND(
    SUM(
      CASE
        WHEN t.annee = :annee - 1 THEN f.marge
        ELSE 0
      END
    ),
    0
  ) AS marge_n1
FROM
  fait_ventes f
  JOIN dim_temps t ON t.id_date = f.id_date
WHERE
  t.annee IN (:annee, :annee - 1)
GROUP BY
  t.mois,
  t.nom_mois
ORDER BY
  t.mois;
-- Paramètre à déclarer : annee (INTEGER, ex: 2025)
  -- La requête calcule automatiquement N et N-1