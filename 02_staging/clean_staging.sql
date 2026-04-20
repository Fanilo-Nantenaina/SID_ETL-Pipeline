USE ventes_stg;
-- ① Normaliser les libellés clients (trim + uppercase / nom propre)
--    Pas de fonction stockée : expression inline compatible phpMyAdmin
--    Résultat : 1ʳᵉ lettre majuscule, reste en minuscule
UPDATE
  stg_clients
SET
  nom = TRIM(UPPER(nom)),
  ville = CONCAT(
    UPPER(LEFT(TRIM(LOWER(ville)), 1)),
    SUBSTRING(TRIM(LOWER(ville)), 2)
  ),
  region = TRIM(region);
-- ② Valeurs nulles : ville et région inconnues
UPDATE
  stg_clients
SET
  ville = 'Non renseigné'
WHERE
  ville IS NULL
  OR ville = '';
UPDATE
  stg_clients
SET
  region = 'Non renseigné'
WHERE
  region IS NULL
  OR region = '';
-- ③ Valeurs nulles : segment
UPDATE
  stg_clients
SET
  segment = 'Particulier'
WHERE
  segment IS NULL
  OR segment NOT IN ('Entreprise', 'Administration', 'Particulier');
-- ④ Date d'entrée invalide → valeur par défaut
UPDATE
  stg_clients
SET
  date_entree = '2020-01-01'
WHERE
  date_entree IS NULL
  OR STR_TO_DATE(date_entree, '%Y-%m-%d') IS NULL;
-- ⑤ Marquer les doublons sur code_client (garder l'id_client le plus élevé)
UPDATE
  stg_clients s
  JOIN (
    SELECT
      code_client,
      MAX(id_client) AS keep_id
    FROM
      stg_clients
    GROUP BY
      code_client
    HAVING
      COUNT(*) > 1
  ) dup ON s.code_client = dup.code_client
  AND s.id_client <> dup.keep_id
SET
  s.stg_status = 'DUPLICATE';
-- ⑥ Marquer les lignes propres
UPDATE
  stg_clients
SET
  stg_status = 'CLEAN'
WHERE
  stg_status = 'PENDING';
-- Vérification
SELECT
  stg_status,
  COUNT(*)
FROM
  stg_clients
GROUP BY
  stg_status;