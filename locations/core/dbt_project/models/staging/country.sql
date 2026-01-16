WITH CTE_CountryDetails AS (
  SELECT
    ID,
    NAME,
    ISO3,
    ISO2,
    CURRENCY,
    REGION,
    REGION_ID,
    SUBREGION,
    SUBREGION_ID,
    -- Replace NULL or empty TIMEZONES with a default value so there's no missing country here if timezone is missing
    CASE
        WHEN TRIM(TIMEZONES) IS NULL OR TRIM(TIMEZONES) = '' 
        THEN '[{"zoneName": "Unknown", "gmtOffsetName": "Unknown"}]'
        ELSE TIMEZONES
    END AS TIMEZONES
  FROM {{ env_var('SNOWFLAKE_DATABASE') }}.{{ env_var('SNOWFLAKE_SCHEMA') }}.COUNTRY_DETAILS
)
SELECT
  ID,
  NAME,
  ISO3,
  ISO2,
  CURRENCY,
  REGION,
  REGION_ID,
  SUBREGION,
  SUBREGION_ID,
  LISTAGG(f.value:zoneName::string, ', ') AS zonename,
  LISTAGG(f.value:gmtOffsetName::string, ', ') AS gmtoffsetname,
  CURRENT_TIMESTAMP AS LAST_UPDATED_DATETIME
FROM CTE_CountryDetails
  , LATERAL FLATTEN(input => PARSE_JSON(TIMEZONES)) AS f
GROUP BY
  ID, NAME, ISO3, ISO2, CURRENCY,
  REGION, REGION_ID, SUBREGION, SUBREGION_ID, LAST_UPDATED_DATETIME