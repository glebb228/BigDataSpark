# BigDataSpark

Лабораторная работа №2 по дисциплине "Анализ больших данных".

В этой работе реализован ETL на Spark:
- исходные CSV загружаются в PostgreSQL;
- в PostgreSQL строится модель "звезда";
- на основе этой модели формируются отчеты в ClickHouse.

## Что есть в репозитории

- `исходные данные/` - 10 CSV-файлов;
- `docker-compose.yml` - PostgreSQL, ClickHouse и Spark;
- `jobs/etl.py` - основная Spark-джоба;
- `sql/postgres/` - создание staging-таблицы, загрузка CSV и создание схемы `mart`;
- `sql/clickhouse/` - настройки пользователя для ClickHouse.

## Что сделано

В PostgreSQL:
- загружается таблица `staging.mock_data`;
- создаются таблицы измерений в схеме `mart`;
- создается таблица фактов `mart.fact_sales`.

В ClickHouse:
- `report_product_sales`
- `report_customer_sales`
- `report_time_sales`
- `report_store_sales`
- `report_supplier_sales`
- `report_product_quality`

Опциональные базы данных из задания не реализовывались, сделана обязательная часть с ClickHouse.

## Запуск

1. Поднять PostgreSQL и ClickHouse:

```bash
docker compose up -d postgres clickhouse
```

2. Запустить Spark-джобу:

```bash
docker compose --profile jobs run --rm spark
```

Если все прошло нормально, в конце будет:

```text
Spark ETL finished successfully
staging rows: 10000
fact rows: 10000
```

## Подключение

PostgreSQL:

```text
host: localhost
port: 5434
database: bd_spark
user: postgres
password: postgres
```

ClickHouse:

```text
host: localhost
port: 8123
database: default
user: bd_spark
password: bd_spark
```

## Проверка

Проверка PostgreSQL:

```bash
docker compose exec -T postgres psql -U postgres -d bd_spark -c "
SELECT 'staging.mock_data' AS table_name, count(*) FROM staging.mock_data
UNION ALL SELECT 'mart.fact_sales', count(*) FROM mart.fact_sales
UNION ALL SELECT 'mart.dim_customer', count(*) FROM mart.dim_customer
UNION ALL SELECT 'mart.dim_product', count(*) FROM mart.dim_product
UNION ALL SELECT 'mart.dim_date', count(*) FROM mart.dim_date
ORDER BY table_name;
"
```

Проверка ClickHouse:

```bash
docker compose exec -T clickhouse clickhouse-client \
  --user bd_spark \
  --password bd_spark \
  --query "
SELECT name, total_rows
FROM system.tables
WHERE database = 'default' AND startsWith(name, 'report_')
ORDER BY name;
"
```

После выполнения должны быть загружены все 10000 строк в staging, 10000 строк в `mart.fact_sales` и созданы 6 отчетов в ClickHouse.

## Сброс данных

Если нужно полностью пересоздать контейнеры и базы:

```bash
docker compose down -v
docker compose up -d postgres clickhouse
docker compose --profile jobs run --rm spark
```
