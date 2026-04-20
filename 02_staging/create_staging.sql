USE ventes_stg;
-- Tables miroir avec colonnes techniques supplémentaires
CREATE TABLE stg_familles (
  id_famille INT,
  libelle VARCHAR(100),
  stg_loaded TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  stg_status VARCHAR(20) DEFAULT 'PENDING' -- PENDING | CLEAN | ERROR
);
CREATE TABLE stg_produits (
  id_produit INT,
  code_produit VARCHAR(30),
  libelle VARCHAR(200),
  id_famille INT,
  marque VARCHAR(100),
  prix_achat DECIMAL(12, 2),
  unite VARCHAR(20),
  stg_loaded TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  stg_status VARCHAR(20) DEFAULT 'PENDING'
);
CREATE TABLE stg_clients (
  id_client INT,
  code_client VARCHAR(20),
  nom VARCHAR(100),
  ville VARCHAR(100),
  region VARCHAR(100),
  segment VARCHAR(50),
  date_entree VARCHAR(30),
  -- stocké en VARCHAR pour capturer les mauvais formats
  stg_loaded TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  stg_status VARCHAR(20) DEFAULT 'PENDING'
);
CREATE TABLE stg_lignes_facture (
  id_ligne INT,
  id_facture INT,
  id_produit INT,
  quantite DECIMAL(12, 3),
  prix_unitaire DECIMAL(12, 2),
  remise_pct DECIMAL(5, 2),
  -- Colonnes jointes depuis factures (pour faciliter le chargement)
  date_facture DATE,
  id_client INT,
  id_commercial INT,
  -- Colonnes calculées (remplies en phase T)
  montant_ht DECIMAL(14, 2),
  montant_ttc DECIMAL(14, 2),
  marge DECIMAL(14, 2),
  remise DECIMAL(14, 2),
  stg_loaded TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  stg_status VARCHAR(20) DEFAULT 'PENDING'
);
CREATE TABLE stg_reglements (
  id_reglement INT,
  id_facture INT,
  id_commercial INT,
  id_mode INT,
  date_reglement DATE,
  montant DECIMAL(12, 2),
  stg_loaded TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  stg_status VARCHAR(20) DEFAULT 'PENDING'
);