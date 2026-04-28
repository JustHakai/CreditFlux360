
USE ROLE SYSADMIN;
USE SCHEMA CREDITFLUX360.SILVER;


CREATE OR REPLACE TABLE CREDITFLUX360.SILVER.SILVER_TRANSACTIONS AS
SELECT
  id_transaction,
  iban_client,                                  -- Déjà haché depuis Bronze
  TRY_TO_DATE(date_operation, 'DD/MM/YYYY') AS date_operation,
  type_operation,
  montant_operation::FLOAT                    AS montant_operation,
  id_contrat_credit,
  code_agence,
  statut_operation,
  motif_rejet,
  -- Colonnes enrichies
  CASE
    WHEN montant_operation::FLOAT < 0
     AND type_operation != 'REMBOURSEMENT'
    THEN TRUE ELSE FALSE
  END AS is_suspicious,
  CURRENT_TIMESTAMP() AS transformed_at

FROM CREDITFLUX360.BRONZE.RAW_TRANSACTIONS

WHERE
  -- Règle 2 : Filtrage temporel
  TRY_TO_DATE(date_operation, 'DD/MM/YYYY') IS NOT NULL
  AND TRY_TO_DATE(date_operation, 'DD/MM/YYYY') <= CURRENT_DATE()
  -- Règle 6 : Dédoublonnage
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id_transaction ORDER BY loaded_at DESC) = 1;


select * from bronze.raw_transactions;

select * from silver.silver_transactions;





-- SILVER SIMULATIONS


