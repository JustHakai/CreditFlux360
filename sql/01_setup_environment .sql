-- 0/Préparation de l'environnement
-- Création de toutes les structures Snowflake nécessaires 
-- avant même que le chargement ne soit considéré

--Statements qui seront uniquement pour débuter ou recommencer
-- Replace pourrait être remplacé par `if not exists` dans la version prod pour éviter un erreur

-- Création de la database et de ses schémas
CREATE OR REPLACE DATABASE  creditflux360;

CREATE OR REPLACE SCHEMA creditflux360.bronze;
CREATE OR REPLACE SCHEMA creditflux360.silver;
CREATE OR REPLACE SCHEMA creditflux360.gold;
CREATE OR REPLACE SCHEMA creditflux360.errors; --schéma pour contenir les différentes tables d'erreur


-- Création des tables bronzes
CREATE OR REPLACE TABLE creditflux360.bronze.raw_transactions(
  id_transaction STRING,
  iban_client STRING,
  date_operation STRING,
  type_operation STRING,
  montant_operation FLOAT,
  id_contrat_credit STRING,
  code_agence STRING,
  statut_operation STRING,
  motif_rejet STRING,
  loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  source_file STRING
);

CREATE OR REPLACE TABLE creditflux360.bronze.raw_simulations(
    raw_data VARIANT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING
);

CREATE OR REPLACE TABLE creditflux360.bronze.raw_contrats(
    raw_data VARIANT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING
);

-- Création des File Formats

-- CSV avec encodage
CREATE OR REPLACE FILE FORMAT FF_CSV_BQVRT
  TYPE = 'CSV'
  FIELD_DELIMITER = ';'
  ENCODING = 'ISO-8859-1'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL')
  EMPTY_FIELD_AS_NULL = TRUE;

-- JSON / NDJSON
CREATE OR REPLACE FILE FORMAT FF_JSON_NDJSON
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = FALSE

-- AVRO

CREATE OR REPLACE FILE FORMAT FF_AVRO
  TYPE = 'AVRO';
  
-- Création du Stage S3

