SELECT
    location_id,
    horizon_hours,
    metric_name,
    forecast_count,
    comparison_count,
    mae,
    rmse,
    bias
FROM {{ ref('forecast_accuracy_by_horizon') }}
WHERE comparison_count < 0
    OR comparison_count > forecast_count
    OR mae < 0
    OR rmse < 0
    OR (
        comparison_count = 0
        AND (mae IS NOT NULL OR rmse IS NOT NULL OR bias IS NOT NULL)
    )
    OR (
        comparison_count > 0
        AND (mae IS NULL OR rmse IS NULL OR bias IS NULL)
    )
