{{
    config(
        materialized="view"
    )
}}

# correcting columns order and adding difference amount from origin (diffOrg) and destination (diffDest)

SELECT
timestamp,
days,
hours,
step,
type,
nameOrig,
oldbalanceOrg,
newbalanceOrig,
ROUND((oldbalanceOrg - newbalanceOrig), 2) AS diffOrg,
nameDest,
oldbalanceDest,
newbalanceDest,
ROUND((oldbalanceDest - newbalanceDest), 2) AS diffDest,
isFraud,
isFlaggedFraud,
FROM
{{ ref('source_table') }}
ORDER BY 
timestamp,
diffOrg DESC