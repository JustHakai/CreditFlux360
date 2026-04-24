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
USE SCHEMA PUBLIC;

-- CSV avec encodage
CREATE OR REPLACE FILE FORMAT FF_CSV
  TYPE = 'CSV'
  FIELD_DELIMITER = ';'
  ENCODING = 'ISO-8859-1'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL')
  EMPTY_FIELD_AS_NULL = TRUE;

-- JSON / NDJSON
CREATE OR REPLACE FILE FORMAT FF_JSON_NDJSON
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = FALSE;

-- AVRO
CREATE OR REPLACE FILE FORMAT FF_AVRO
  TYPE = 'AVRO';

-- Création du Stage S3

-- Le bucket S3 étant encrypté, il nous faut un IAM Role que AWS reconnaitra pour y accéder
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION
  banqueverte_s3_iam
  TYPE = EXTERNAL_STAGE
  ENABLED = TRUE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::565265042247:role/snowflake_ro_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://banqueverte-landing-565265042247-us-west-2-an/');
-- Il est nécessaire de modifier les trust policies du role dans AWS si le Storage Integration est recréé
-- voir le résulat 
DESC INTEGRATION banqueverte_s3_iam;

-- Il est temps de redevenir SYSADMIN, maia avant il faut se donner le droit d'utiliser l'(ntégraton d'abord

-- GRANT CREATE STAGE ON SCHEMA public TO ROLE ;

GRANT USAGE ON INTEGRATION banqueverte_s3_iam TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

CREATE or replace STAGE banqueverte_s3
  STORAGE_INTEGRATION = banqueverte_s3_iam
  URL = 's3://banqueverte-landing-565265042247-us-west-2-an/'
  FILE_FORMAT = FF_CSV;

LIST @banqueverte_s3;