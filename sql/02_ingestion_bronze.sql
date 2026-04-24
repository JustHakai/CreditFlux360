


-- Transactions

COPY INTO RAW_TRANSACTIONS
FROM ()

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