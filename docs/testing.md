# Тестирование

| Уровень | Что проверяем | Как запустить |
|---|---|---|
| Unit | Логику Python, разбор ответов, формирование событий и интервалы исторической загрузки | `docker build --target test -t weather-ingestion-tests ./ingestion` и `docker run --rm weather-ingestion-tests` |
| Data Quality | Ключи, связи, контракт событий, интервалы, расчёт ошибок и метрик | `dbt test` из контейнера Airflow или DAG `weather_dwh_transform` |
| End-to-End | Прохождение реальных событий от погодного источника до PostgreSQL и запросов Grafana | Вручную запустить DAG сбора и трансформации, затем проверить Kafka, Raw, Core и панели |

## Быстрая проверка стенда

```bash
./scripts/smoke-test.sh
```

Smoke-test собирает образ с unit-тестами, проверяет синтаксис JSON дашбордов, импорт DAG, структуру проекта dbt, материализованные данные и доступность API Grafana. `dbt parse` проверяет проект, а `dbt test` проверяет уже построенные модели. После изменения моделей или поступления новых событий дождитесь успешного запуска `weather_dwh_transform`. Запускайте smoke-test после трансформации: проверка полноты обнаружит события, которые уже появились в Raw и ещё ожидают обработки.

`scripts/check-data.sql` работает в транзакции только для чтения. Он проверяет ключи в Core и витринах, формулу температурной ошибки и права роли Grafana. Результат включает число строк и статусы свежести. Пустые таблицы допустимы на чистом стенде. Для подтверждения сбора нужны реальные события.

```bash
docker compose exec -T postgres \
  sh -c 'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < scripts/check-data.sql
```

## Сквозная проверка

1. Снимите счётчики `raw.forecast_events` и `raw.observation_events`. Запустите `weather_forecast_collection`. Проверьте Kafka offsets и дождитесь появления опубликованных сообщений в Raw. При обычном запуске ожидается по одному сообщению на город.
2. Запустите `weather_observations_collection`. Он назначает станции и публикует доступные почасовые наблюдения. Дождитесь поступления событий в Raw.
3. Запустите `weather_dwh_transform`. Проверьте появление живых снимков и прогнозных часов, уникальность измерений по станции и целевому часу и прохождение всех тестов dbt.
4. Для проверки архива дозагрузите период с доступными прогнозами и наблюдениями. Повторите трансформацию и проверьте ненулевой счётчик температурных сравнений для города в `dm.fact_archive_forecast_accuracy`. [Команды исторической дозагрузки](backfill.md)
5. Проверьте **Weather Forecast Quality**, **Historical Forecast Accuracy** и **Weather Pipeline Health**. Меняйте город и период. Панели ошибок могут быть пустыми в период без совпавших прогнозов и измерений. JSON-проверка проверяет синтаксис, а выполнение SQL подтверждается запросами через Grafana.

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9092 --topic weather.forecasts.raw
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9092 --topic weather.observations.raw
```

## Восстановление

- Запомните число строк Raw и Core. Перезапустите Spark командой `docker compose restart spark`. Проверьте в логах запуск обоих потоков и восстановление прежних checkpoints. Выполните ещё один сбор и убедитесь, что новые сообщения появились в Raw. Сравните счётчики и повторите `scripts/check-data.sql`.
- Перезапустите Airflow командой `docker compose restart airflow-api-server airflow-scheduler airflow-dag-processor`. Проверьте доступность DAG, сохранение расписаний, состояния запусков и журналов.
- Повторите backfill прогнозов и наблюдений за уже обработанный день одного города. После Spark и dbt Raw может вырасти. Сравните число уникальных архивных выпусков и измерений Core с результатом до повторной загрузки и проверьте отсутствие дублей.

Для чистой проверки поднимите отдельный проект Compose с новыми томами и выполните bootstrap, smoke-test и сквозной сценарий. Инструкции по изоляции и остановке стенда находятся в [руководстве по развёртыванию](deployment.md).
