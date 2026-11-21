# Историческая дозагрузка Meteostat

Команда `weather-observations-backfill` загружает почасовые наблюдения за указанные даты. Для каждого города она берёт текущее назначение из `control.station_assignments`. Перед публикацией проверьте, что у выбранных городов сохранены назначения станций и что в Compose работают PostgreSQL и Kafka.

## Запуск

Соберите образ с CLI.

```bash
docker compose -f docker-compose.yaml build ingestion
```

Проверьте план за октябрь. Для пяти городов с размером окна 7 дней команда выведет 25 окон. В режиме `--dry-run` CLI читает файл городов и выводит план локально.

```bash
docker compose -f docker-compose.yaml run --rm -T \
  --entrypoint weather-observations-backfill ingestion \
  --locations /app/config/locations.json \
  --start 2026-10-01 --end 2026-10-31 --chunk-days 7 --dry-run
```

Пробный сбор за один день выведет события в JSONL. Диагностика и итоговая статистика идут в stderr.

```bash
docker compose -f docker-compose.yaml run --rm -T \
  --entrypoint weather-observations-backfill ingestion \
  --locations /app/config/locations.json \
  --start 2026-10-01 --end 2026-10-01 --city moscow --output jsonl
```

Для загрузки в конвейер укажите `--output kafka`. После поступления событий в Raw запустите `weather_dwh_transform` в Airflow или дождитесь следующего планового запуска.

```bash
docker compose -f docker-compose.yaml run --rm -T \
  --entrypoint weather-observations-backfill ingestion \
  --locations /app/config/locations.json \
  --start 2026-10-01 --end 2026-10-31 --chunk-days 7 \
  --sleep-seconds 1 --output kafka
```

## Параметры

| Параметр | Назначение |
| --- | --- |
| `--start`, `--end` | Первая и последняя даты в UTC включительно, формат `YYYY-MM-DD`. |
| `--locations` | Путь к существующему `locations.json`. |
| `--city` | Идентификатор одного города. По умолчанию выбираются все города из файла. |
| `--chunk-days` | От 1 до 31 дней на окно. По умолчанию 7. |
| `--output` | `jsonl` для просмотра или `kafka` для отправки в конвейер. По умолчанию `jsonl`. |
| `--dry-run` | Вывод плана с полуоткрытыми границами в UTC. |
| `--sleep-seconds` | Пауза между запросами Meteostat. По умолчанию 1 секунда. |

Для Kafka используются `KAFKA_BOOTSTRAP_SERVERS` и `KAFKA_OBSERVATION_TOPIC`. По умолчанию это `kafka:9092` и `weather.observations.raw`.

## Повторная загрузка и ограничения

- После ошибки окна команда обрабатывает оставшиеся окна и возвращает ненулевой код при ошибках обработки или доставки. Окна без измерений учитываются отдельно как `empty`.
- Повторный запуск отправляет новые события в Kafka и Raw. Core хранит одну запись на пару «станция — час» и выбирает наиболее свежую версию измерения.
- Поле `station.assigned_at_utc` сохраняет реальное время назначения текущей станции. Оно может быть позже запрошенного периода. Для сопоставления с прогнозами требуется назначение, действовавшее на целевой час.
- Доступность измерений зависит от станции и периода. Пропущенные часы остаются пропущенными, модельное заполнение отключено.
- События помечаются `ingestion_mode=backfill` в Raw и Staging. В `dm.pipeline_freshness` для Meteostat учитываются только регулярные события.


## Архивные прогнозы Open-Meteo

Команда `weather-forecasts-backfill` загружает отдельные выпуски ECMWF IFS (`ecmwf_ifs`) в 00:00, 06:00, 12:00 и 18:00 UTC. Для каждого выпуска и города запрос задаёт 168 почасовых значений в GMT. Ответ вместе с массивами и единицами сохраняется в поле `forecast`. [Документация Single Runs API](https://open-meteo.com/en/docs/single-runs-api)

`model_run_at_utc` хранит историческое время инициализации модели. `collected_at_utc` фиксирует, когда CLI получил прогноз. События с `forecast_kind=archived_run` поступают в существующий Kafka-топик `weather.forecasts.raw` и сохраняются в Raw. Live-модель dbt пропускает только `forecast_kind=live` и старые события без поля. Архивная обработка в Core будет отдельным этапом.

```bash
docker compose -f docker-compose.yaml run --rm -T \
  --entrypoint weather-forecasts-backfill ingestion \
  --locations /app/config/locations.json \
  --start 2026-10-01 --end 2026-10-03 --dry-run
```

| Параметр | Назначение |
| --- | --- |
| `--start`, `--end` | Первый и последний дни инициализации включительно, формат `YYYY-MM-DD` в UTC. |
| `--locations` | Путь к `locations.json`. По умолчанию `/app/config/locations.json`. |
| `--city` | Идентификатор города. По умолчанию выбираются все города. |
| `--output` | `jsonl` для вывода событий или `kafka` для загрузки в Raw. По умолчанию `jsonl`. |
| `--dry-run` | Показать план запросов без обращений к Open-Meteo и Kafka. |
| `--sleep-seconds` | Пауза между запросами. По умолчанию 1 секунда. |

Для пробного сбора за один день задайте одинаковые `--start` и `--end`, укажите `--city moscow --output jsonl` и уберите `--dry-run`. Перед публикацией примените обновлённую Staging-модель через `weather_dwh_transform`. Для публикации используйте `--output kafka`. Адрес брокера и топик задаются через `KAFKA_BOOTSTRAP_SERVERS` и `KAFKA_FORECAST_TOPIC`.

CLI продолжит работу после ошибки отдельного запроса и вернёт ненулевой код при ошибках загрузки или доставки. При повторной загрузке успешные выпуски появятся в Raw повторно. Их можно различать по ключу `(location_id, model_id, model_run_at_utc)`.
