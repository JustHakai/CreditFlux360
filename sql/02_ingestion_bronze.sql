
 truncate table bronze.raw_transactions;
 truncate table errors.bronze_load_errors;
 truncate table bronze.stag_transactions;


CREATE OR REPLACE FUNCTION CREDITFLUX360.BRONZE.HASH_IBAN(iban VARCHAR, salt VARCHAR)
  RETURNS VARCHAR
  AS
  $$
    SHA2(CONCAT(iban, salt), 256)
  $$;

-- Transactions
BEGIN
-- Création d'une table de staging des données (temporaire)
    CREATE OR REPLACE TRANSIENT TABLE CREDITFLUX360.BRONZE.STAG_TRANSACTIONS (
      id_transaction  VARCHAR,
      iban            VARCHAR,
      date_operation  DATE,
      type_operation  VARCHAR,
      montant_operation NUMBER,
      id_contrat_credit VARCHAR,
      code_agence     VARCHAR,
      statut_operation VARCHAR,
      motif_rejet     VARCHAR,
      source_file     VARCHAR
    );
    
    -- copie des données en brut dans la table 0
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
      FROM @CREDITFLUX360.PUBLIC.BANQUEVERTE_S3/flux_transactions_20240315.csv
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
    TRUNCATE TABLE CREDITFLUX360.BRONZE.STAG_TRANSACTIONS;
    
    
    -- select * from bronze.raw_transactions;


    RETURN 'Ingestion complete';

END;



select * from  errors.bronze_load_errors
WHERE error_message not like 'Date%';









-- Simulations

COPY INTO RAW_SIMULATIONS (raw_data, loaded_at, source_file)
FROM (
  SELECT
    $1,
    CURRENT_TIMESTAMP(),
    METADATA$FILENAME
  FROM @CREDITFLUX360.PUBLIC.banqueverte_s3/app_simulations_credit_v1.json
  (FILE_FORMAT => 'CREDITFLUX360.PUBLIC.FF_JSON_NDJSON')
)
ON_ERROR = 'CONTINUE';


select * from bronze.raw_simulations;









-- Contrats

COPY INTO CREDITFLUX360.BRONZE.RAW_CONTRATS (raw_data, loaded_at, source_file)
FROM (
  SELECT
    $1,                          -- Tout l'enregistrement Avro dans VARIANT
    CURRENT_TIMESTAMP(),
    METADATA$FILENAME
  FROM @PUBLIC.BANQUEVERTE_S3/contrats_credit_full.avro
)
FILE_FORMAT = (FORMAT_NAME = 'FF_AVRO')
ON_ERROR = 'CONTINUE';

