"""
DAG для обработки событий агентских сервисов из Kafka:
1. Создаёт таблицы (если не существуют)
2. Получает сообщения из Kafka
3. Сохраняет сырые сообщения во временную таблицу
4. Разбирает и сохраняет в основную таблицу
5. Обновляет статус обработки
6. Очищает старые обработанные данные
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pendulum import datetime
from airflow.hooks.base import BaseHook
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from sqlalchemy import create_engine, text
import uuid
import logging

# Константы
CONN_ID_KAP = 'kap_247_db'
KAFKA_TOPIC_C = "SCPL.BULKAGENTSERVICESEVENT.V1"
SCHEMA_NAME = "kap_247_scpl"
TMP_TABLE = "bulkagentservicesevent_tmp"
MAIN_TABLE = "BULKAGENTSERVICESEVENT"
DATA_RETENTION_DAYS = 30  # Хранение данных во временной таблице
PROCESSING_BATCH_SIZE = 500  # Размер пачки для обработки

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# SQL для создания таблиц
CREATE_TMP_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TMP_TABLE} (
    id SERIAL PRIMARY KEY,
    message_key VARCHAR(255) NOT NULL,
    text_json JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP WITH TIME ZONE,
    processing_status VARCHAR(20) DEFAULT 'NEW'
);
"""

CREATE_MAIN_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{MAIN_TABLE} (
    id SERIAL PRIMARY KEY,
    tmp_record_id INTEGER REFERENCES {SCHEMA_NAME}.{TMP_TABLE}(id),
    event_type VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    tenant_id UUID NOT NULL,
    division_id UUID NOT NULL,
    division_name VARCHAR(255),
    service_id UUID NOT NULL,
    service_name VARCHAR(255),
    message_id UUID NOT NULL,
    service_kind VARCHAR(50),
    user_id UUID NOT NULL,
    tenant_user_id UUID NOT NULL,
    proficiency_level INTEGER,
    connection_type VARCHAR(50),
    interaction_search_tactic VARCHAR(50),
    last_name VARCHAR(100),
    first_name VARCHAR(100),
    patronymic VARCHAR(100),
    employee_id VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_{MAIN_TABLE}_tmp_id ON {SCHEMA_NAME}.{MAIN_TABLE}(tmp_record_id);
CREATE INDEX IF NOT EXISTS idx_{MAIN_TABLE}_timestamp ON {SCHEMA_NAME}.{MAIN_TABLE}(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_{MAIN_TABLE}_tenant ON {SCHEMA_NAME}.{MAIN_TABLE}(tenant_id);
"""

def _get_engine(conn_id_str):
    """Создаёт и возвращает SQLAlchemy engine"""
    conn = BaseHook.get_connection(conn_id_str)
    return create_engine(
        f'postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}',
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        pool_recycle=3600
    )

def _ensure_tables_exist():
    """Создаёт таблицы если они не существуют"""
    engine = _get_engine(CONN_ID_KAP)
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))
        conn.execute(text(CREATE_TMP_TABLE_SQL))
        conn.execute(text(CREATE_MAIN_TABLE_SQL))
    engine.dispose()

def consume_function(message, rqUid):
    """Обрабатывает сообщение из Kafka и сохраняет во временную таблицу"""
    engine = _get_engine(CONN_ID_KAP)
    try:
        if not message or not hasattr(message, 'value'):
            logger.error('Получено некорректное сообщение')
            return

        msg_value = message.value()
        if not msg_value:
            logger.warning('Получено пустое сообщение')
            return

        decoded_message = msg_value.decode('utf-8')
        message_key = message.key().decode('utf-8') if message.key() else str(uuid.uuid4())

        # Логирование информации о сообщении
        logger.info(f"Получено сообщение. Key: {message_key}, Size: {len(decoded_message)} bytes")

        with engine.begin() as conn:
            conn.execute(
                text(f"""
                INSERT INTO {SCHEMA_NAME}.{TMP_TABLE} 
                (message_key, text_json, processing_status)
                VALUES (:key, :message, 'PROCESSED')
                """),
                {"key": message_key, "message": decoded_message}
            )
    except Exception as e:
        logger.error(f"Ошибка обработки сообщения: {str(e)}", exc_info=True)
    finally:
        engine.dispose()

@task
def process_messages():
    """Переносит данные из временной таблицы в основную"""
    engine = _get_engine(CONN_ID_KAP)
    
    with engine.begin() as conn:
        # Логирование статистики перед обработкой
        total = conn.execute(text(f"""
            SELECT COUNT(*) 
            FROM {SCHEMA_NAME}.{TMP_TABLE}
            WHERE processing_status = 'PROCESSED'
            AND processed_at IS NULL
        """)).scalar()
        logger.info(f"Найдено {total} записей для обработки")

        if total == 0:
            logger.info("Нет новых записей для обработки")
            return

        # Обработка пачками
        processed = 0
        while processed < total:
            batch = conn.execute(text(f"""
                WITH batch AS (
                    SELECT id, text_json
                    FROM {SCHEMA_NAME}.{TMP_TABLE}
                    WHERE processing_status = 'PROCESSED'
                    AND processed_at IS NULL
                    ORDER BY created_at
                    LIMIT {PROCESSING_BATCH_SIZE}
                    FOR UPDATE SKIP LOCKED
                )
                INSERT INTO {SCHEMA_NAME}.{MAIN_TABLE} (
                    tmp_record_id, event_type, event_timestamp, tenant_id, 
                    division_id, division_name, service_id, service_name,
                    message_id, service_kind, user_id, tenant_user_id,
                    proficiency_level, connection_type, interaction_search_tactic,
                    last_name, first_name, patronymic, employee_id
                )
                SELECT 
                    batch.id,
                    batch.text_json::jsonb->'payload'->>'event_type',
                    (batch.text_json::jsonb->'payload'->>'timestamp')::timestamp,
                    (batch.text_json::jsonb->'payload'->>'tenant_id')::uuid,
                    (batch.text_json::jsonb->'payload'->>'division_id')::uuid,
                    batch.text_json::jsonb->'payload'->>'division_name',
                    (batch.text_json::jsonb->'payload'->>'service_id')::uuid,
                    batch.text_json::jsonb->'payload'->>'service_name',
                    (batch.text_json::jsonb->'payload'->>'message_id')::uuid,
                    batch.text_json::jsonb->'payload'->>'service_kind',
                    (user_info->>'user_id')::uuid,
                    (user_info->>'tenant_user_id')::uuid,
                    (user_info->>'proficiency_level')::integer,
                    user_info->>'connection_type',
                    user_info->>'interaction_search_tactic',
                    user_info->>'last_name',
                    user_info->>'first_name',
                    user_info->>'patronymic',
                    user_info->>'employee_id'
                FROM 
                    batch,
                    jsonb_array_elements(batch.text_json::jsonb->'payload'->'user_info') user_info
                RETURNING tmp_record_id
            """)).rowcount

            # Обновление статуса для обработанных записей
            updated = conn.execute(text(f"""
                UPDATE {SCHEMA_NAME}.{TMP_TABLE}
                SET processed_at = CURRENT_TIMESTAMP
                WHERE id IN (
                    SELECT id 
                    FROM {SCHEMA_NAME}.{TMP_TABLE}
                    WHERE processing_status = 'PROCESSED'
                    AND processed_at IS NULL
                    ORDER BY created_at
                    LIMIT {PROCESSING_BATCH_SIZE}
                    FOR UPDATE SKIP LOCKED
                )
            """)).rowcount

            processed += updated
            logger.info(f"Обработано {updated} записей. Всего: {processed}/{total}")

@task
def cleanup_old_data():
    """Очищает старые обработанные данные"""
    engine = _get_engine(CONN_ID_KAP)
    with engine.begin() as conn:
        deleted = conn.execute(text(f"""
            DELETE FROM {SCHEMA_NAME}.{TMP_TABLE}
            WHERE processed_at < NOW() - INTERVAL '{DATA_RETENTION_DAYS} days'
            RETURNING id
        """)).rowcount
        logger.info(f"Удалено {deleted} старых записей")

@dag(
    start_date=datetime(2025, 4, 9),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    concurrency=4,
    render_template_as_native_obj=True,
    tags=['kafka', 'postgres', 'scpl', 'production'],
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(minutes=30)
    }
)
def job_kafka_read_json4_prod():
    @task
    def setup_database():
        """Создаёт таблицы если они не существуют"""
        _ensure_tables_exist()
    
    @task
    def generate_rq_uid():
        """Генерирует уникальный ID для отслеживания"""
        return str(uuid.uuid4())
    
    consume_task = ConsumeFromTopicOperator(
        task_id="consume",
        kafka_config_id="kafka_synapce",
        topics=[KAFKA_TOPIC_C],
        apply_function=consume_function,
        apply_function_kwargs={"rqUid": "{{ ti.xcom_pull(task_ids='generate_rq_uid')}}"},
        commit_cadence="end_of_batch",
        max_messages=PROCESSING_BATCH_SIZE * 2,
        max_batch_size=PROCESSING_BATCH_SIZE,
        poll_timeout=30
    )

    # Порядок выполнения задач
    setup_db = setup_database()
    rq_uid = generate_rq_uid()
    process_task = process_messages()
    cleanup_task = cleanup_old_data()
    
    setup_db >> rq_uid >> consume_task >> process_task >> cleanup_task

job_kafka_read_json4_prod()