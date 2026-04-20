-- Dataset R4 : Évolution mensuelle N vs N-1
-- Paramètre : annee (INTEGER, ex: 2025)
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
  ) AS evolution_pct
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