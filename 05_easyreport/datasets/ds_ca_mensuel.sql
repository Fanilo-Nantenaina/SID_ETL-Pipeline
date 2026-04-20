-- Dataset R1 : Chiffre d'affaires mensuel par famille de produits
-- Paramètres : annee_debut (INTEGER), annee_fin (INTEGER)
SELECT
  t.annee,
  t.mois,
  t.nom_mois,
  p.famille AS famille_produit,
  ROUND(SUM(f.montant_ht), 0) AS ca_ht,
  ROUND(SUM(f.quantite), 1) AS quantite_totale,
  ROUND(SUM(f.marge), 0) AS marge_totale,
  ROUND(
    SUM(f.marge) / NULLIF(SUM(f.montant_ht), 0) * 100,
    1
  ) AS taux_marge_pct
FROM
  fait_ventes f
  JOIN dim_temps t ON t.id_date = f.id_date
  JOIN dim_produit p ON p.id_produit = f.id_produit
WHERE
  t.annee BETWEEN :annee_debut
  AND :annee_fin
GROUP BY
  t.annee,
  t.mois,
  t.nom_mois,
  p.famille
ORDER BY
  t.annee,
  t.mois,
  p.famille;