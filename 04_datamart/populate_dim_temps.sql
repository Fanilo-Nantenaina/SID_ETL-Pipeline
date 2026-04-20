-- 04_datamart/populate_dim_temps.sql
-- Génération du calendrier dim_temps pour la période 2022-2026
-- À exécuter UNE SEULE FOIS, avant le premier chargement de fait_ventes.
-- (L'ETL Python fait la même chose via load_dim_temps() — ce script est
--  l'alternative purement SQL si l'on préfère ne pas passer par Python.)
USE ventes_dm;
-- Sécurité : ne rien faire si déjà peuplé
SELECT
  COUNT(*) INTO @n
FROM
  dim_temps;
-- (Commenter le bloc suivant si l'on veut forcer un rechargement)
INSERT INTO
  dim_temps (
    id_date,
    jour,
    mois,
    trimestre,
    semestre,
    annee,
    nom_mois,
    jour_semaine,
    est_jour_ouvre
  ) WITH RECURSIVE cal AS (
    SELECT
      DATE('2022-01-01') AS d
    UNION ALL
    SELECT
      DATE_ADD(d, INTERVAL 1 DAY)
    FROM
      cal
    WHERE
      d < '2026-12-31'
  )
SELECT
  CAST(DATE_FORMAT(d, '%Y%m%d') AS UNSIGNED) AS id_date,
  DAY(d) AS jour,
  MONTH(d) AS mois,
  QUARTER(d) AS trimestre,
  IF(MONTH(d) <= 6, 1, 2) AS semestre,
  YEAR(d) AS annee,
  DATE_FORMAT(d, '%M') AS nom_mois,
  DAYOFWEEK(d) - 1 AS jour_semaine,
  -- 1=Lun … 7=Dim (ISO)
  DAYOFWEEK(d) NOT IN (1, 7) AS est_jour_ouvre
FROM
  cal
WHERE
  @n = 0;
-- insert uniquement si table vide
SELECT
  COUNT(*) AS nb_jours_inseres
FROM
  dim_temps;
-- Attendu : 1826 (2022-01-01 au 2026-12-31 inclus)