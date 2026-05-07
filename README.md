# BigDataSpark

Анализ больших данных — лабораторная работа №2: ETL на **Apache Spark**.

![Схема](https://github.com/user-attachments/assets/2b854382-4c36-4542-a7fb-04fe82a6f6fa)

**Цель:** загрузить исходные данные из 10 CSV в PostgreSQL (staging), сформировать модель «звезда» в PostgreSQL, затем построить 6 витрин и сохранить их в ClickHouse.

---

### Состав репозитория

| Файл | Зачем |
|------|--------|
| `.env.example` | Пример переменных окружения (копируется в `.env`) |
| `docker-compose.yml` | Подъём PostgreSQL + Spark + ClickHouse |
| `sql/postgres/01_stage_load.sql` | Загрузка CSV в `stage.mock_data` (ожидается 10000 строк) |
| `spark/jobs/01_build_star_postgres.py` | Spark-джоба: `stage.mock_data` → модель «звезда» `dw.*` в PostgreSQL |
| `spark/jobs/02_reports_to_clickhouse.py` | Spark-джоба: `dw.*` → 6 витрин `mart_*` в ClickHouse |
| `sql/clickhouse/01_init.sql` | Инициализация базы ClickHouse (по умолчанию `analytics`) |
| `исходные данные/` | Исходные CSV: `MOCK_DATA.csv` … `MOCK_DATA (9).csv` |

---

### Запуск

Требуется запущенный Docker. Один раз подготовьте файл окружения:

```bash
cd BigDataSpark
cp .env.example .env
docker compose up -d --build
```

Запуск Spark-джоб:

```bash
docker exec -it bigdata-spark-spark spark-submit /opt/spark/jobs/01_build_star_postgres.py
docker exec -it bigdata-spark-spark spark-submit /opt/spark/jobs/02_reports_to_clickhouse.py
```

---

### Проверка результата

PostgreSQL (staging и модель «звезда»):

```bash
docker exec -it bigdata-spark-postgres psql -U bigdata -d bigdata_spark
```

```sql
SELECT COUNT(*) FROM stage.mock_data;  -- 10000
SELECT COUNT(*) FROM dw.fact_sales;    -- 10000
```

ClickHouse (6 витрин):

```bash
docker exec -it bigdata-spark-clickhouse clickhouse-client --query "SHOW TABLES FROM analytics"
```

Ожидаемые таблицы витрин:

- `mart_sales_by_products`
- `mart_sales_by_customers`
- `mart_sales_by_time`
- `mart_sales_by_stores`
- `mart_sales_by_suppliers`
- `mart_product_quality`

---

### Пересоздание окружения

SQL-инициализация контейнеров выполняется при создании томов данных. Для полного пересоздания:

```bash
docker compose down -v
docker compose up -d --build
```
