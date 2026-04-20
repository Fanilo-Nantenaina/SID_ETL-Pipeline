-- Dataset R2 : Top 10 clients par CA sur une année
-- Paramètre : annee (INTEGER)
SELECT
  c.nom AS client,
  c.segment,
  c.ville,
  ROUND(SUM(f.montant_ht), 0) AS ca_total,
  COUNT(DISTINCT f.id_date) AS nb_jours_achat,
  ROUND(AVG(f.montant_ht), 0) AS panier_moyen,
  ROUND(
    SUM(f.montant_ht) / (
      SELECT
        SUM(fv2.montant_ht)
      FROM
        fait_ventes fv2
        JOIN dim_temps tv ON tv.id_date = fv2.id_date
      WHERE
        tv.annee = :annee
    ) * 100,
    1
  ) AS part_pct
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
ORDER BY
  ca_total DESC
LIMIT
  10;