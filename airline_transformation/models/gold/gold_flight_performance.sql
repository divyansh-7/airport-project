{{ config(
    description="Aggregated flight-level performance metrics analyzing baggage processing efficiency and delay percentages."
) }}

WITH silver_data AS (
    -- The ref function creates an automatic dependency pipeline in dbt
    SELECT * FROM {{ ref('silver_baggage_tracking') }}
),

flight_aggregations AS (
    SELECT
        flight_number,
        passenger_gate,
        
        -- Count total operations
        COUNT(DISTINCT pnr_locator) AS total_passengers_processed,
        COUNT(DISTINCT bag_tag_number) AS total_bags_tracked,
        
        -- Performance Metrics
        AVG(scan_to_board_interval_minutes) AS avg_bag_scan_to_boarding_duration,
        MAX(scan_to_board_interval_minutes) AS max_bag_scan_delay,
        
        -- Count how many bags were processed after boarding completed
        SUM(CASE WHEN is_late_baggage_scan = TRUE THEN 1 ELSE 0 END) AS total_late_bags,
        
        -- Capture the latest tracking window for auditing
        MAX(baggage_time) AS last_processed_at
    FROM silver_data
    GROUP BY 1, 2
)

SELECT
    *,
    -- Calculate the crucial SLA KPI: What percentage of bags missed the passenger boarding window?
    CASE 
        WHEN total_bags_tracked > 0 
        THEN ROUND((total_late_bags / total_bags_tracked) * 100, 2)
        ELSE 0 
    END AS baggage_mishandle_rate_percentage
FROM flight_aggregations
ORDER BY baggage_mishandle_rate_percentage DESC