{{
    config(
        materialized='table',
        partition_by={
            'field': 'timestamp',
            'data_type': 'timestamp',
            'granularity': 'hour'
        },
        cluster_by = 'type'
    )
}}

  SELECT
    *
  FROM
    {{ ref('tidying_the_columns') }}