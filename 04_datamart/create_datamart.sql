USE ventes_dm;
-- ── DIM_TEMPS ─────────────────────────────────────
CREATE TABLE dim_temps (
  id_date INT PRIMARY KEY,
  -- YYYYMMDD ex: 20240315
  jour TINYINT,
  mois TINYINT,
  trimestre TINYINT,
  semestre TINYINT,
  annee SMALLINT,
  nom_mois VARCHAR(20),
  jour_semaine TINYINT,
  -- 1=Lundi … 7=Dimanche
  est_jour_ouvre BOOLEAN
);
-- ── DIM_CLIENT ────────────────────────────────────
CREATE TABLE dim_client (
  id_client INT AUTO_INCREMENT PRIMARY KEY,
  -- Clé de substitution
  code VARCHAR(20),
  nom VARCHAR(100),
  ville VARCHAR(100),
  region VARCHAR(100),
  segment VARCHAR(50),
  date_entree DATE,
  date_debut DATE NOT NULL,
  date_fin DATE,
  -- NULL = version active
  est_actuel BOOLEAN DEFAULT TRUE
);
CREATE INDEX idx_cli_code ON dim_client(code);
-- ── DIM_PRODUIT ───────────────────────────────────
CREATE TABLE dim_produit (
  id_produit INT AUTO_INCREMENT PRIMARY KEY,
  code VARCHAR(30),
  libelle VARCHAR(200),
  sous_famille VARCHAR(100),
  famille VARCHAR(100),
  marque VARCHAR(100),
  unite VARCHAR(20)
);
-- ── DIM_COMMERCIAL ────────────────────────────────
CREATE TABLE dim_commercial (
  id_commercial INT AUTO_INCREMENT PRIMARY KEY,
  nom VARCHAR(100),
  equipe VARCHAR(100),
  date_embauche DATE
);
-- ── DIM_MODE_REGLEMENT ────────────────────────────
CREATE TABLE dim_mode_reglement (
  id_mode INT AUTO_INCREMENT PRIMARY KEY,
  libelle VARCHAR(50)
);
-- ── FAIT_VENTES ───────────────────────────────────
CREATE TABLE fait_ventes (
  id_fait BIGINT AUTO_INCREMENT PRIMARY KEY,
  id_date INT NOT NULL,
  id_client INT NOT NULL,
  id_produit INT NOT NULL,
  id_commercial INT NOT NULL,
  quantite DECIMAL(12, 3),
  montant_ht DECIMAL(14, 2),
  montant_ttc DECIMAL(14, 2),
  marge DECIMAL(14, 2),
  remise DECIMAL(14, 2),
  INDEX idx_fv_date (id_date),
  INDEX idx_fv_cli (id_client),
  INDEX idx_fv_prod (id_produit),
  INDEX idx_fv_comm (id_commercial)
);
-- ── FAIT_REGLEMENTS ───────────────────────────────
CREATE TABLE fait_reglements (
  id_fait_reg BIGINT AUTO_INCREMENT PRIMARY KEY,
  id_date INT NOT NULL,
  id_client INT NOT NULL,
  id_commercial INT NOT NULL,
  id_mode INT NOT NULL,
  montant_regle DECIMAL(14, 2),
  INDEX idx_fr_date (id_date),
  INDEX idx_fr_cli (id_client)
);