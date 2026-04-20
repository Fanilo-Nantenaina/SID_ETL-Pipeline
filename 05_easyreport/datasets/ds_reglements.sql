-- ============================================================
-- 05_easyreport/datasets/ds_reglements.sql
-- Rapport R3 : Suivi des règlements par commercial et client
-- Paramètre  : annee (INTEGER)
-- ============================================================
SELECT
  com.nom AS commercial,
  c.nom AS client,
  c.segment,
  m.libelle AS mode_reglement,
  ROUND(SUM(fv.montant_ttc), 0) AS montant_facture,
  ROUND(COALESCE(SUM(fr.montant_regle), 0), 0) AS montant_regle,
  ROUND(
    SUM(fv.montant_ttc) - COALESCE(SUM(fr.montant_regle), 0),
    0
  ) AS solde_du,
  ROUND(
    COALESCE(SUM(fr.montant_regle), 0) / NULLIF(SUM(fv.montant_ttc), 0) * 100,
    1
  ) AS taux_recouvrement_pct
FROM
  fait_ventes fv
  JOIN dim_client c ON c.id_client = fv.id_client
  AND c.est_actuel = TRUE
  JOIN dim_commercial com ON com.id_commercial = fv.id_commercial
  JOIN dim_temps t ON t.id_date = fv.id_date
  LEFT JOIN fait_reglements fr ON fr.id_client = fv.id_client
  AND fr.id_commercial = fv.id_commercial
  LEFT JOIN dim_mode_reglement m ON m.id_mode = fr.id_mode
WHERE
  t.annee = :annee
GROUP BY
  com.nom,
  c.nom,
  c.segment,
  m.libelle
ORDER BY
  solde_du DESC;
-- Paramètre à déclarer : annee (INTEGER, ex: 2024)