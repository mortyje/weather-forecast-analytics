# Развёртывание локального стенда

Нужны Docker с Compose v2, Bash, Python 3 и curl. На Windows команды выполняются из WSL с подключением к Docker Desktop. При первой сборке нужен доступ к реестрам образов и пакетов. Spark загружает Kafka-коннектор при первом запуске и кеширует его в томе `spark_ivy`.

## Подготовка окружения

Клонируйте репозиторий и создайте локальный файл конфигурации.

```bash
cp .env.example .env
```

Заполните все пустые секреты. Для `AIRFLOW_FERNET_KEY` нужен URL-safe Base64 ключ из 32 байт. Для `AIRFLOW_DB_PASSWORD` удобно использовать шестнадцатеричный пароль, который можно вставить в строку подключения как есть. Для остальных паролей и ключей создайте отдельные значения.

```bash
python3 -c 'import base64, secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())'
openssl rand -hex 32
```

Используйте значения только в локальном `.env`. Пример в Git содержит пустые поля для секретов. `AIRFLOW_UID` задаёт пользователя контейнеров Airflow.

## Запуск

```bash
./scripts/bootstrap.sh
./scripts/smoke-test.sh
```

Bootstrap проверяет настройки и запускает сервисы. Compose ждёт готовности PostgreSQL и Kafka. Одноразовые сервисы создают Raw и таблицу назначений станций, создают топики и мигрируют базу Airflow. После сборки моделей dbt отдельный сервис выдаёт Grafana права чтения на схему `dm`. Затем запускаются проверки dbt. Миграции можно применить повторно на сохранённом стенде.

```bash
docker compose ps -a
docker compose logs --tail=100 spark airflow-scheduler grafana
curl --fail http://127.0.0.1:3000/api/health
```

Одноразовые сервисы инициализации должны завершиться с кодом 0. Airflow доступен по адресу [http://127.0.0.1:8080](http://127.0.0.1:8080). Вход для пользователя `admin` использует пароль Simple Auth Manager. При первом запуске Airflow записывает его в том авторизации и выводит в локальный журнал. Grafana доступна по адресу [http://127.0.0.1:3000](http://127.0.0.1:3000). Для первого входа используются `admin` и `GRAFANA_ADMIN_PASSWORD`. Дашборды находятся в папке **Weather Analytics**.

PostgreSQL, Kafka и Spark публикуют порты для локальной диагностики. Все опубликованные порты привязаны к `127.0.0.1`.

## Второй экземпляр и остановка

Для чистой проверки используйте отдельную копию репозитория. В её `.env` задайте уникальный `COMPOSE_PROJECT_NAME` и свободные `POSTGRES_PORT`, `AIRFLOW_PORT`, `GRAFANA_PORT`, `KAFKA_PORT` и `SPARK_PORT`. Имя проекта отделяет сеть и имена томов. [Документация Docker Compose](https://docs.docker.com/compose/how-tos/project-name/)

```bash
docker compose restart spark
docker compose stop
docker compose down
```

`restart` перезапускает выбранный сервис. `stop` оставляет контейнеры, а `down` останавливает и удаляет контейнеры и сеть. Обе команды сохраняют именованные тома. Удаление томов через `down --volumes` уничтожает хранилище PostgreSQL и Kafka, checkpoints Spark, авторизацию и журналы Airflow и данные Grafana. Для штатного перезапуска удаление томов не требуется.
