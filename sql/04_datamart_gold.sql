


-- Création de la vue métier finale

CREATE OR REPLACE VIEW CREDITFLUX360.GOLD.GOLD_ANALYSE_RISQUE_CREDIT AS

WITH transactions_aggregees AS (
  -- Compter les incidents par contrat
  SELECT
    id_contrat_credit,
    COUNT(*) AS nb_transactions,
    SUM(CASE WHEN type_operation = 'INCIDENT' THEN 1 ELSE 0 END) AS nb_incidents,
    MAX(CASE WHEN type_operation = 'INCIDENT' THEN 1 ELSE 0 END) = 1 AS has_incident
  FROM CREDITFLUX360.SILVER.SILVER_TRANSACTIONS
  GROUP BY id_contrat_credit
),

simulations_par_contrat AS (
  -- Dernière simulation connue par contrat
  SELECT
    contract_id,
    montant_souhaite,
    mensualite_estimee
  FROM CREDITFLUX360.SILVER.SILVER_SIMULATIONS
  QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY timestamp_utc DESC) = 1
)

SELECT
  c.type_contrat,
  c.gamme_contrat,
  c.classe_eba,
  COUNT(DISTINCT c.client_id)                              AS nb_contrats,
  AVG(c.taux_effort)                                       AS taux_effort_moyen,
  SUM(CASE WHEN c.is_risque_eleve THEN 1 ELSE 0 END)      AS nb_risque_eleve,
  -- NPL Ratio : part des contrats avec au moins un incident
  ROUND(
    SUM(CASE WHEN t.has_incident THEN 1 ELSE 0 END)
    / NULLIF(COUNT(DISTINCT c.client_id), 0),
    4
  )                                                        AS npl_ratio,
  -- Comparaison simulation vs accordé
  AVG(s.montant_souhaite)                                  AS montant_simule_moyen,
  AVG(c.montant_accorde)                                   AS montant_accorde_moyen,
  AVG(c.montant_accorde - s.montant_souhaite)              AS ecart_moyen_simulation_accord

FROM CREDITFLUX360.SILVER.SILVER_CONTRATS c
LEFT JOIN transactions_aggregees t     ON c.client_id = t.id_contrat_credit
LEFT JOIN simulations_par_contrat s    ON c.client_id = s.contract_id

GROUP BY c.type_contrat, c.gamme_contrat, c.classe_eba;


