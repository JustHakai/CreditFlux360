

-- CREATION DES TABLES BRONZES
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


-- Création des tables d'erreur
CREATE TABLE IF NOT EXISTS creditflux360.errors.bronze_load_errors (
  source_file     VARCHAR,
  rejected_record VARCHAR,
  error_message   VARCHAR,
  error_column    VARCHAR,
  row_number      NUMBER,
  loaded_at       TIMESTAMP
);


-- Création de la table de staging  et de sa masking policy

CREATE OR REPLACE MASKING POLICY CREDITFLUX360.BRONZE.MASK_IBAN
AS (val VARCHAR) RETURNS VARCHAR ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN val
    ELSE '****-****-MASKED'
  END;

-- Création d'une table de staging des données (temporaire)
CREATE OR REPLACE TRANSIENT TABLE CREDITFLUX360.BRONZE.STAG_TRANSACTIONS (
    id_transaction  VARCHAR,
    iban            VARCHAR WITH MASKING POLICY CREDITFLUX360.BRONZE.MASK_IBAN, 
    date_operation  VARCHAR,
    type_operation  VARCHAR,
    montant_operation NUMBER,
    id_contrat_credit VARCHAR,
    code_agence     VARCHAR,
    statut_operation VARCHAR,
    motif_rejet     VARCHAR,
    source_file     VARCHAR
);

CREATE OR REPLACE TRANSIENT TABLE CREDITFLUX360.BRONZE.STAG_SIMULATIONS (
    raw_data    VARIANT,
    source_file VARCHAR
);

 truncate table bronze.raw_transactions;
 truncate table errors.bronze_load_errors;
 truncate table bronze.stag_transactions;
 truncate table bronze.stag_simulations;


CREATE OR REPLACE FUNCTION CREDITFLUX360.BRONZE.HASH_IBAN(iban VARCHAR, salt VARCHAR)
  RETURNS VARCHAR
  AS
  $$
    SHA2(CONCAT(iban, salt), 256)
  $$;



--
----
-------
----------
-------------- CHARGEMENT DES DONNEES 
----------
-------
----
--



-- Transactions
BEGIN
    -- copie des données en brut dans la table de staging
    COPY INTO CREDITFLUX360.BRONZE.STAG_TRANSACTIONS
    (
    iban,
    id_transaction,
    date_operation,
    type_operation,
    montant_operation,
    id_contrat_credit,
    code_agence,
    statut_operation,
    motif_rejet,
    source_file
    )
    FROM (
      SELECT
        $2  AS iban,
        $1  AS id_transaction,
        $3  AS date_operation,
        $4  AS type_operation,
        $5  AS montant_operation,
        $6  AS id_contrat_credit,
        $7  AS code_agence,
        $8  AS statut_operation,
        $9  AS motif_rejet,
        METADATA$FILENAME AS source_file
      FROM @CREDITFLUX360.BRONZE.BANQUEVERTE_S3/flux_transactions_20240315.csv
    )
    FILE_FORMAT = (FORMAT_NAME = 'FF_CSV')
    ON_ERROR = 'CONTINUE';
    

    --log load errors into error table
    INSERT INTO CREDITFLUX360.errors.BRONZE_LOAD_ERRORS
    SELECT
      FILE            AS source_file,
      REJECTED_RECORD AS rejected_record,
      ERROR           AS error_message,
      COLUMN_NAME     AS error_column,
      ROW_NUMBER      AS row_number,
      CURRENT_TIMESTAMP() AS loaded_at
    FROM TABLE(
      VALIDATE(
        CREDITFLUX360.BRONZE.STAG_TRANSACTIONS,
        JOB_ID => '_last'
      )
    );

    
    -- insert into bronze transactions table
    INSERT INTO CREDITFLUX360.BRONZE.RAW_TRANSACTIONS
    SELECT
      id_transaction,
      CREDITFLUX360.BRONZE.HASH_IBAN(iban, UUID_STRING()) AS iban_client,
      date_operation,
      type_operation,
      montant_operation,
      id_contrat_credit,
      code_agence,
      statut_operation,
      motif_rejet,
      CURRENT_TIMESTAMP()  AS loaded_at,
      source_file
    FROM CREDITFLUX360.BRONZE.STAG_TRANSACTIONS;
    
    
    -- trucate to clear the raw data
    -- TRUNCATE TABLE CREDITFLUX360.BRONZE.STAG_TRANSACTIONS;
    
    
    -- select * from bronze.raw_transactions;


    RETURN 'Ingestion complete';

END;



select * from  errors.bronze_load_errors
-- WHERE error_message not like 'Date%'
;

select * from bronze.raw_transactions;







-- Simulations
COPY INTO CREDITFLUX360.BRONZE.STAG_SIMULATIONS (raw_data, source_file)
FROM (
  SELECT
    $1,
    METADATA$FILENAME
  FROM @CREDITFLUX360.BRONZE.banqueverte_s3/app_simulations_credit_v1.json
  (FILE_FORMAT => 'CREDITFLUX360.BRONZE.FF_RAW_LINES')
)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;


-- COPY INTO CREDITFLUX360.BRONZE.STAG_SIMULATIONS (raw_data, source_file)
-- FROM (
--   SELECT
--     $1,
--     METADATA$FILENAME
--   FROM @CREDITFLUX360.BRONZE.banqueverte_s3/app_simulations_credit_v1.json
--   (FILE_FORMAT => 'CREDITFLUX360.BRONZE.FF_JSON_NDJSON')
-- )
-- ON_ERROR = 'CONTINUE'
-- FORCE = TRUE;

-- INSERT INTO CREDITFLUX360.BRONZE.RAW_SIMULATIONS (raw_data, loaded_at, source_file)
-- SELECT
--   raw_data,
--   CURRENT_TIMESTAMP(),
--   source_file
-- FROM CREDITFLUX360.BRONZE.STAG_SIMULATIONS;

--copy from staging to raw simulation

INSERT INTO CREDITFLUX360.BRONZE.RAW_SIMULATIONS (raw_data, loaded_at, source_file)
SELECT
  TRY_PARSE_JSON(raw_data),
  CURRENT_TIMESTAMP(),
  source_file
FROM CREDITFLUX360.BRONZE.STAG_SIMULATIONS
WHERE TRY_PARSE_JSON(raw_data) IS NOT NULL
  AND TRIM(raw_data) != '';

--load errors

INSERT INTO CREDITFLUX360.ERRORS.BRONZE_LOAD_ERRORS (
  source_file, rejected_record, error_message, error_column, row_number, loaded_at
)
SELECT
  source_file,
  raw_data,
  'Invalid JSON - parse failed',
  NULL,
  NULL,
  CURRENT_TIMESTAMP()
FROM CREDITFLUX360.BRONZE.STAG_SIMULATIONS
WHERE TRY_PARSE_JSON(raw_data) IS NULL
  AND TRIM(raw_data) != '';


INSERT INTO CREDITFLUX360.errors.BRONZE_LOAD_ERRORS
SELECT
    FILE            AS source_file,
    REJECTED_RECORD AS rejected_record,
    ERROR           AS error_message,
    COLUMN_NAME     AS error_column,
    ROW_NUMBER      AS row_number,
    CURRENT_TIMESTAMP() AS loaded_at
FROM TABLE(
    VALIDATE(
    CREDITFLUX360.BRONZE.STAG_SIMULATIONS,
    JOB_ID => '_last'
    )
);


truncate table bronze.raw_simulations;

select * from bronze.raw_simulations
;
select * from errors.bronze_load_errors;








-- Contrats

COPY INTO CREDITFLUX360.BRONZE.RAW_CONTRATS (raw_data, loaded_at, source_file)
FROM (
  SELECT
    $1,                          -- Tout l'enregistrement Avro dans VARIANT
    CURRENT_TIMESTAMP(),
    METADATA$FILENAME
  FROM @BRONZE.BANQUEVERTE_S3/contrats_credit_full.avro
)
FILE_FORMAT = (FORMAT_NAME = 'FF_AVRO')
ON_ERROR = 'CONTINUE';


select * from bronze.raw_contrats;








