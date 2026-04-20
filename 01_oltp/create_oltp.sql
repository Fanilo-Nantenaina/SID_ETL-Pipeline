USE ventes_oltp;
CREATE TABLE familles (
  id_famille INT AUTO_INCREMENT PRIMARY KEY,
  libelle VARCHAR(100) NOT NULL
);
CREATE TABLE produits (
  id_produit INT AUTO_INCREMENT PRIMARY KEY,
  code_produit VARCHAR(30) NOT NULL UNIQUE,
  libelle VARCHAR(200) NOT NULL,
  id_famille INT NOT NULL,
  marque VARCHAR(100),
  prix_achat DECIMAL(12, 2) DEFAULT 0,
  unite VARCHAR(20) DEFAULT 'unité',
  FOREIGN KEY (id_famille) REFERENCES familles(id_famille)
);
CREATE TABLE clients (
  id_client INT AUTO_INCREMENT PRIMARY KEY,
  code_client VARCHAR(20) NOT NULL UNIQUE,
  nom VARCHAR(100) NOT NULL,
  ville VARCHAR(100),
  region VARCHAR(100),
  segment ENUM('Entreprise', 'Administration', 'Particulier') DEFAULT 'Particulier',
  date_entree DATE NOT NULL
);
CREATE TABLE commerciaux (
  id_commercial INT AUTO_INCREMENT PRIMARY KEY,
  nom VARCHAR(100) NOT NULL,
  equipe VARCHAR(100),
  date_embauche DATE
);
CREATE TABLE modes_reglement (
  id_mode INT AUTO_INCREMENT PRIMARY KEY,
  libelle VARCHAR(50) NOT NULL
);
CREATE TABLE factures (
  id_facture INT AUTO_INCREMENT PRIMARY KEY,
  numero VARCHAR(30) NOT NULL UNIQUE,
  date_facture DATE NOT NULL,
  id_client INT NOT NULL,
  id_commercial INT NOT NULL,
  FOREIGN KEY (id_client) REFERENCES clients(id_client),
  FOREIGN KEY (id_commercial) REFERENCES commerciaux(id_commercial)
);
CREATE TABLE lignes_facture (
  id_ligne INT AUTO_INCREMENT PRIMARY KEY,
  id_facture INT NOT NULL,
  id_produit INT NOT NULL,
  quantite DECIMAL(12, 3) NOT NULL,
  prix_unitaire DECIMAL(12, 2) NOT NULL,
  remise_pct DECIMAL(5, 2) DEFAULT 0,
  FOREIGN KEY (id_facture) REFERENCES factures(id_facture),
  FOREIGN KEY (id_produit) REFERENCES produits(id_produit)
);
CREATE TABLE reglements (
  id_reglement INT AUTO_INCREMENT PRIMARY KEY,
  id_facture INT NOT NULL,
  id_commercial INT NOT NULL,
  id_mode INT NOT NULL,
  date_reglement DATE NOT NULL,
  montant DECIMAL(12, 2) NOT NULL,
  FOREIGN KEY (id_facture) REFERENCES factures(id_facture),
  FOREIGN KEY (id_commercial) REFERENCES commerciaux(id_commercial),
  FOREIGN KEY (id_mode) REFERENCES modes_reglement(id_mode)
);