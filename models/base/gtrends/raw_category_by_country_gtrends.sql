SELECT
    CAST (date AS DATE) AS date,
    geo AS country,
    time,
    gprop AS source,
    category,
    CAST(hits AS INT64) AS hits

from {{ source('imports','countries_v2') }}