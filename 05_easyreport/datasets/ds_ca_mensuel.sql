-- ============================================================
-- 05_easyreport/datasets/ds_ca_mensuel.sql
-- Rapport R1 : CA mensuel par famille de produits
-- Paramètres : annee_debut (INTEGER), annee_fin (INTEGER)
-- Exemple    : annee_debut=2023, annee_fin=2025
-- ============================================================
SELECT
  t.annee,
  t.mois,
  t.nom_mois,
  p.famille AS famille_produit,
  ROUND(SUM(f.montant_ht), 0) AS ca_ht,
  ROUND(SUM(f.quantite), 1) AS quantite_totale,
  ROUND(SUM(f.marge), 0) AS marge_totale,
  ROUND(SUM(f.remise), 0) AS remise_totale,
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
-- Paramètres à déclarer dans EasyReport :
  --   annee_debut  type INTEGER
  --   annee_fin    type INTEGER