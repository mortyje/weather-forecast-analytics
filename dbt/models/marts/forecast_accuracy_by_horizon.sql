WITH metric_errors AS (
    SELECT
        facts.location_id,
        facts.horizon_hours,
        metrics.metric_name,
        metrics.unit,
        metrics.forecast_value - metrics.actual_value AS error
    FROM {{ ref('fact_forecast_accuracy') }} AS facts
    CROSS JOIN LATERAL (
        VALUES
            (
                'temperature',
                '°C',
                facts.temperature_forecast_c::NUMERIC,
                facts.temperature_actual_c::NUMERIC
            ),
            (
                'humidity',
                '%',
                facts.humidity_forecast_pct::NUMERIC,
                facts.humidity_actual_pct::NUMERIC
            ),
            (
                'precipitation',
                'mm',
                facts.precipitation_forecast_mm::NUMERIC,
                facts.precipitation_actual_mm::NUMERIC
            ),
            (
                'wind_speed',
                'km/h',
                facts.wind_forecast_kmh::NUMERIC,
                facts.wind_actual_kmh::NUMERIC
            )
    ) AS metrics(metric_name, unit, forecast_value, actual_value)
)

SELECT
    location_id,
    horizon_hours,
    metric_name,
    unit,
    COUNT(*) AS forecast_count,
    COUNT(error) AS comparison_count,
    ROUND(AVG(ABS(error)), 3) AS mae,
    ROUND(SQRT(AVG(POWER(error, 2))), 3) AS rmse,
    ROUND(AVG(error), 3) AS bias
FROM metric_errors
GROUP BY location_id, horizon_hours, metric_name, unit
