-- ============================================================
-- 04_datamart/validate_datamart.sql
-- Requêtes de contrôle qualité du datamart ventes_dm
-- IMPORTANT phpMyAdmin : sélectionner la base "ventes_dm"
-- dans le panneau de gauche AVANT d'exécuter ce script.
-- Toutes les tables sont préfixées ventes_dm. pour éviter
-- l'erreur "table not found" lors des requêtes cross-base.
-- ============================================================
-- ① Volumes des tables
SELECT
  'dim_temps' AS table_dm,
  COUNT(*) AS nb
FROM
  ventes_dm.dim_temps
UNION ALL
SELECT
  'dim_client',
  COUNT(*)
FROM
  ventes_dm.dim_client
UNION ALL
SELECT
  'dim_produit',
  COUNT(*)
FROM
  ventes_dm.dim_produit
UNION ALL
SELECT
  'dim_commercial',
  COUNT(*)
FROM
  ventes_dm.dim_commercial
UNION ALL
SELECT
  'dim_mode',
  COUNT(*)
FROM
  ventes_dm.dim_mode_reglement
UNION ALL
SELECT
  'fait_ventes',
  COUNT(*)
FROM
  ventes_dm.fait_ventes
UNION ALL
SELECT
  'fait_reglements',
  COUNT(*)
FROM
  ventes_dm.fait_reglements;
-- Attendu : ~1826 / 60 / 150 / 4 / 4 / ~8000 / ~2400
  -- ② Clés étrangères orphelines dans fait_ventes (doit retourner 0)
SELECT
  COUNT(*) AS orphelins_date
FROM
  ventes_dm.fait_ventes
WHERE
  id_date NOT IN (
    SELECT
      id_date
    FROM
      ventes_dm.dim_temps
  );
SELECT
  COUNT(*) AS orphelins_client
FROM
  ventes_dm.fait_ventes
WHERE
  id_client NOT IN (
    SELECT
      id_client
    FROM
      ventes_dm.dim_client
  );
SELECT
  COUNT(*) AS orphelins_produit
FROM
  ventes_dm.fait_ventes
WHERE
  id_produit NOT IN (
    SELECT
      id_produit
    FROM
      ventes_dm.dim_produit
  );
SELECT
  COUNT(*) AS orphelins_commercial
FROM
  ventes_dm.fait_ventes
WHERE
  id_commercial NOT IN (
    SELECT
      id_commercial
    FROM
      ventes_dm.dim_commercial
  );
-- ③ Contrôle croisé OLTP ↔ Datamart
  --    Les deux CA doivent être identiques (écart < 1 Ariary)
SELECT
  'OLTP' AS source,
  ROUND(
    SUM(
      l.quantite * l.prix_unitaire * (1 - l.remise_pct / 100)
    ),
    0
  ) AS ca_ht
FROM
  ventes_oltp.lignes_facture l
UNION ALL
SELECT
  'Datamart',
  ROUND(SUM(montant_ht), 0)
FROM
  ventes_dm.fait_ventes;
-- ④ CA par année dans le datamart
SELECT
  t.annee,
  ROUND(SUM(f.montant_ht), 0) AS ca_ht,
  ROUND(SUM(f.marge), 0) AS marge_totale,
  COUNT(*) AS nb_lignes
FROM
  ventes_dm.fait_ventes f
  JOIN ventes_dm.dim_temps t ON t.id_date = f.id_date
GROUP BY
  t.annee
ORDER BY
  t.annee;
-- ⑤ Vérification de la saisonnalité (T4 doit être le plus élevé)
SELECT
  t.trimestre,
  ROUND(SUM(f.montant_ht), 0) AS ca_ht
FROM
  ventes_dm.fait_ventes f
  JOIN ventes_dm.dim_temps t ON t.id_date = f.id_date
GROUP BY
  t.trimestre
ORDER BY
  t.trimestre;
-- ⑥ Vérification fait_reglements (orphelins date et client)
SELECT
  COUNT(*) AS orphelins_date_regl
FROM
  ventes_dm.fait_reglements
WHERE
  id_date NOT IN (
    SELECT
      id_date
    FROM
      ventes_dm.dim_temps
  );
SELECT
  COUNT(*) AS orphelins_client_regl
FROM
  ventes_dm.fait_reglements
WHERE
  id_client NOT IN (
    SELECT
      id_client
    FROM
      ventes_dm.dim_client
  );
-- ⑦ Taux de recouvrement global (fait_reglements / fait_ventes)
SELECT
  ROUND(SUM(fr.montant_regle), 0) AS total_regle,
  ROUND(SUM(fv.montant_ttc), 0) AS total_facture,
  ROUND(
    SUM(fr.montant_regle) / SUM(fv.montant_ttc) * 100,
    1
  ) AS taux_recouvrement_pct
FROM
  ventes_dm.fait_ventes fv
  CROSS JOIN (
    SELECT
      SUM(montant_regle) AS montant_regle
    FROM
      ventes_dm.fait_reglements
  ) fr;
-- Attendu : environ 80% (20% de factures non réglées dans le générateur)