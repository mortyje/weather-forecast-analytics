WITH metric_values AS (
    SELECT
        facts.location_id,
        facts.model_id,
        facts.model_horizon_hours,

        metrics.metric_name,
        metrics.unit,
        metrics.forecast_value,
        metrics.actual_value

    FROM {{ ref('fact_archive_forecast_accuracy') }} AS facts

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
    ) AS metrics(
        metric_name,
        unit,
        forecast_value,
        actual_value
    )
)

SELECT
    location_id,
    model_id,
    model_horizon_hours,
    metric_name,
    unit,

    COUNT(*) AS forecast_count,

    COUNT(*) FILTER (
        WHERE forecast_value IS NOT NULL
            AND actual_value IS NOT NULL
    ) AS comparison_count,

    ROUND(
        AVG(ABS(forecast_value - actual_value)),
        3
    ) AS mae,

    ROUND(
        SQRT(
            AVG(POWER(forecast_value - actual_value, 2))
        ),
        3
    ) AS rmse,

    ROUND(
        AVG(forecast_value - actual_value),
        3
    ) AS bias

FROM metric_values

GROUP BY
    location_id,
    model_id,
    model_horizon_hours,
    metric_name,
    unit
