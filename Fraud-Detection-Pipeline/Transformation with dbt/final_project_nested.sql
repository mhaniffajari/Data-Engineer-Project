{{
    config(
        materialized='table'
    )
}}

WITH finalproject_nested AS (
    SELECT
    timestamp,
    ARRAY_AGG(STRUCT(days,
        hours,
        step,
        type,
        type_nested)) AS timestamp_nested
  FROM (
    SELECT
      timestamp,
      days,
      hours,
      step,
      type,
      ARRAY_AGG(STRUCT(nameOrig,
          oldbalanceOrg,
          newbalanceOrig,
          diffOrg,
          nameDest,
          oldbalanceDest,
          newbalanceDest,
          diffDest,
          isFraud,
          isFlaggedFraud)) AS type_nested
    FROM
      {{ ref('tidying_the_columns') }}
    GROUP BY
      type,
      step,
      hours,
      days,
      timestamp)
  GROUP BY
    timestamp
  ORDER BY
    timestamp)

SELECT * FROM finalproject_nested