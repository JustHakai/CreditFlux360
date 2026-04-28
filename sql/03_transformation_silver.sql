
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


CREATE OR REPLACE TABLE CREDITFLUX360.SILVER.SILVER_SIMULATIONS AS
SELECT
  raw_data:trace_id::STRING                              AS trace_id,
  raw_data:timestamp_utc::TIMESTAMP_TZ                   AS timestamp_utc,
  raw_data:contract_id::STRING                           AS contract_id,
  raw_data:user_hash::STRING                             AS user_hash,
  raw_data:simulation_details.type_credit::STRING        AS type_credit,
  raw_data:simulation_details.montant_souhaite::FLOAT    AS montant_souhaite,
  raw_data:simulation_details.duree_mois::INT            AS duree_mois,
  raw_data:simulation_details.taux_annuel::FLOAT         AS taux_annuel,
  raw_data:simulation_details.mensualite_estimee::FLOAT  AS mensualite_estimee,
  CURRENT_TIMESTAMP()                                    AS transformed_at

FROM CREDITFLUX360.BRONZE.RAW_SIMULATIONS

WHERE
  -- Règle 4 : Seulement les simulations CONSO avec contract_id valide
  raw_data:simulation_details.type_credit::STRING = 'CONSO'
  AND raw_data:contract_id IS NOT NULL
  AND raw_data:contract_id::STRING != '';


select * from silver.silver_simulations;



-- SILVER CONTRATS 

CREATE OR REPLACE TABLE CREDITFLUX360.SILVER.SILVER_CONTRATS AS
SELECT
  raw_data:client_id::STRING          AS client_id,
  raw_data:iban_hash::STRING          AS iban_hash,
  raw_data:nom_hash::STRING           AS nom_hash,
  TRY_TO_DATE(raw_data:date_naissance::STRING, 'YYYY-MM-DD') AS date_naissance,
  raw_data:revenu_mensuel_net::FLOAT  AS revenu_mensuel_net,
  raw_data:type_contrat::STRING       AS type_contrat,
  raw_data:gamme_contrat::STRING      AS gamme_contrat,
  raw_data:montant_accorde::FLOAT     AS montant_accorde,
  raw_data:mensualite::FLOAT          AS mensualite,
  TRY_TO_DATE(raw_data:date_debut::STRING, 'YYYY-MM-DD') AS date_debut,
  TRY_TO_DATE(raw_data:date_fin::STRING, 'YYYY-MM-DD')   AS date_fin,
  -- Objet scoring aplati
  raw_data:scoring.score_interne::INT    AS score_interne,
  raw_data:scoring.classe_eba::STRING    AS classe_eba,
  raw_data:scoring.is_npl::BOOLEAN       AS is_npl,
  -- Calcul du taux d'effort (règle 1)
  ROUND(
    raw_data:mensualite::FLOAT / NULLIF(raw_data:revenu_mensuel_net::FLOAT, 0),
    4
  ) AS taux_effort,
  CASE
    WHEN raw_data:mensualite::FLOAT / NULLIF(raw_data:revenu_mensuel_net::FLOAT, 0) > 0.33
    THEN TRUE ELSE FALSE
  END AS is_risque_eleve,
  CURRENT_TIMESTAMP() AS transformed_at

FROM CREDITFLUX360.BRONZE.RAW_CONTRATS

WHERE raw_data:revenu_mensuel_net::FLOAT > 0
  AND raw_data:montant_accorde::FLOAT > 0;




select * from silver.silver_contrats;


