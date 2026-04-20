USE ventes_stg;
-- ══════════════════════════════════════════════════════════════════
-- Fonction nom_propre (1ʳᵉ lettre de chaque mot en majuscule)
-- ══════════════════════════════════════════════════════════════════
DROP FUNCTION IF EXISTS nom_propre;
DELIMITER $ $ CREATE FUNCTION nom_propre(str VARCHAR(200)) RETURNS VARCHAR(200) DETERMINISTIC BEGIN DECLARE result VARCHAR(200) DEFAULT '';
DECLARE word VARCHAR(200);
DECLARE rest VARCHAR(200);
DECLARE sep_pos INT;
SET
  rest = TRIM(LOWER(str));
WHILE LENGTH(rest) > 0 DO
SET
  sep_pos = LOCATE(' ', rest);
IF sep_pos = 0 THEN
SET
  word = rest;
SET
  rest = '';
  ELSE
SET
  word = LEFT(rest, sep_pos - 1);
SET
  rest = LTRIM(SUBSTRING(rest, sep_pos + 1));
END IF;
IF LENGTH(result) > 0 THEN
SET
  result = CONCAT(result, ' ');
END IF;
SET
  result = CONCAT(result, UPPER(LEFT(word, 1)), SUBSTRING(word, 2));
END WHILE;
RETURN result;
END $ $ DELIMITER;
-- ══════════════════════════════════════════════════════════════════
-- Nettoyage stg_clients
-- ══════════════════════════════════════════════════════════════════
-- ① Normaliser les libellés clients (trim + uppercase)
UPDATE
  stg_clients
SET
  nom = TRIM(UPPER(nom)),
  ville = TRIM(nom_propre(ville)),
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