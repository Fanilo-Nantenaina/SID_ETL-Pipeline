-- Dataset R5 : KPI globaux pour le tableau de bord de direction
-- Paramètre : annee (INTEGER)
SELECT
  ROUND(SUM(f.montant_ht), 0) AS ca_ht_total,
  ROUND(SUM(f.montant_ttc), 0) AS ca_ttc_total,
  ROUND(SUM(f.marge), 0) AS marge_totale,
  ROUND(
    SUM(f.marge) / NULLIF(SUM(f.montant_ht), 0) * 100,
    1
  ) AS taux_marge_pct,
  ROUND(SUM(f.remise), 0) AS remise_totale,
  ROUND(
    SUM(f.remise) / NULLIF(SUM(f.montant_ht) + SUM(f.remise), 0) * 100,
    1
  ) AS taux_remise_pct,
  COUNT(DISTINCT f.id_client) AS nb_clients_actifs,
  COUNT(*) AS nb_lignes_vente,
  ROUND(
    SUM(f.montant_ht) / NULLIF(COUNT(DISTINCT f.id_client), 0),
    0
  ) AS ca_moyen_par_client,
  ROUND(
    SUM(f.montant_ht) / NULLIF(COUNT(DISTINCT f.id_date), 0),
    0
  ) AS ca_moyen_par_jour
FROM
  fait_ventes f
  JOIN dim_temps t ON t.id_date = f.id_date
WHERE
  t.annee = :annee;