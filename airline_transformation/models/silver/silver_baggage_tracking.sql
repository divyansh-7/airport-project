{{ config(
    description="Cleansed and conformed flight telemetry data with standard timestamps and calculated baggage processing intervals."
) }}

WITH raw_source AS (
    -- The source function sets up our dynamic lineage path
    SELECT * FROM {{ source('raw_airline_telemetry', 'BRONZE_B2B_JOINED') }}
),

cleansed_telemetry AS (
    SELECT
        -- Standardize text strings to uppercase to eliminate casing friction
        UPPER(TRIM(pnr_locator)) AS pnr_locator,
        UPPER(TRIM(flight_number)) AS flight_number,
        UPPER(TRIM(bag_tag_number)) AS bag_tag_number,
        UPPER(TRIM(baggage_location)) AS baggage_location,
        UPPER(TRIM(passenger_gate)) AS passenger_gate,
        
        -- Explicitly cast timestamps to guarantee clean temporal analysis
        CAST(baggage_time AS TIMESTAMP_NTZ) AS baggage_time,
        CAST(passenger_time AS TIMESTAMP_NTZ) AS passenger_time,
        
        -- Pull out the partition variables for chronological tracking
        year,
        month,
        day
    FROM raw_source
    -- Protect downstream layers by ensuring no completely empty rows sneak past
    WHERE pnr_locator IS NOT NULL 
      AND bag_tag_number IS NOT NULL
)

SELECT
    *,
    -- Replace the old DATEDIFF line with this high-fidelity calculation:
(DATEDIFF('second', passenger_time, baggage_time) / 60.0) AS scan_to_board_interval_minutes,

-- Consider adding an operational buffer to your business logic flag:
-- A bag is only "late" if it's scanned more than 1 minute (60 seconds) after boarding
CASE 
    WHEN DATEDIFF('second', passenger_time, baggage_time) > 60 THEN TRUE 
    ELSE FALSE 
END AS is_late_baggage_scan
FROM cleansed_telemetry