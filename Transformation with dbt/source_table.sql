{{
    config(
        materialized="view"
    )
}}

SELECT * FROM `data-fellowship-7-363107.final_project.final_project_with_timestamp`
ORDER BY timestamp, step