from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from sqlalchemy import create_engine, text
import uuid
import logging
from typing import Dict, Any

# Импортируем функцию из отдельного модуля
from kafka_utils import consume_function

# Константы
CONN_ID_KAP = 'kap_247_db'
KAFKA_TOPIC_C = "SCPL.BULKAGENTSERVICESEVENT.V1"
SCHEMA_NAME = "kap_247_scpl"
TMP_TABLE = "bulkagentservicesevent_tmp"
MAIN_TABLE = "BULKAGENTSERVICESEVENT"
DATA_RETENTION_DAYS = 30
BATCH_LIMIT = 50000
CHUNK_SIZE = 5000
OFFSET_STORAGE_KEY = f"{KAFKA_TOPIC_C}_last_offset"
DEFAULT_OFFSET = "earliest"

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# SQL для создания таблиц (остается без изменений)
CREATE_TMP_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TMP_TABLE} (
    id SERIAL PRIMARY KEY,
    message_key VARCHAR(255) NOT NULL,
    text_json JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP WITH TIME ZONE,
    processing_status VARCHAR(20) DEFAULT 'NEW',
    kafka_offset BIGINT
);
CREATE INDEX IF NOT EXISTS idx_{TMP_TABLE}_offset ON {SCHEMA_NAME}.{TMP_TABLE}(kafka_offset);
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
CREATE INDEX IF NOT EXISTS idx_{MAIN_TABLE}_timestamp ON {SCHEMA_NAME}.{MAIN_TABLE}(event_timestamp);
"""

def _get_engine(conn_id_str: str):
    """Создаёт engine для подключения к PostgreSQL"""
    conn = BaseHook.get_connection(conn_id_str)
    return create_engine(
        f'postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'
    )

def _ensure_tables_exist():
    """Создаёт таблицы с дополнительными индексами"""
    engine = _get_engine(CONN_ID_KAP)
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))
        conn.execute(text(CREATE_TMP_TABLE_SQL))
        conn.execute(text(CREATE_MAIN_TABLE_SQL))
    engine.dispose()

def get_last_offset() -> str:
    """Возвращает последний успешно обработанный offset"""
    return Variable.get(OFFSET_STORAGE_KEY, default_var=DEFAULT_OFFSET)

def store_last_offset(offset: int):
    """Сохраняет последний обработанный offset в Variable"""
    Variable.set(OFFSET_STORAGE_KEY, str(offset))

def consume_function(messages: list, **kwargs) -> Dict[str, Any]:
    """Обрабатывает пачку сообщений и возвращает статистику"""
    rqUid = kwargs.get('rqUid', str(uuid.uuid4()))
    
    if not messages:
        return {'processed': 0, 'last_offset': None}
    
    engine = _get_engine(CONN_ID_KAP)
    processed = 0
    last_offset = None
    
    try:
        with engine.begin() as conn:
            records = []
            for message in messages:
                if not message or not hasattr(message, 'value'):
                    continue
                
                msg_value = message.value()
                if not msg_value:
                    continue
                
                try:
                    decoded_message = msg_value.decode('utf-8')
                    message_key = message.key().decode('utf-8') if message.key() else str(uuid.uuid4())
                    records.append({
                        "key": message_key,
                        "message": decoded_message,
                        "offset": message.offset()
                    })
                    last_offset = message.offset()
                except Exception as e:
                    logger.error(f"Ошибка декодирования сообщения: {str(e)}")
                    continue
            
            if records:
                conn.execute(
                    text(f"""
                    INSERT INTO {SCHEMA_NAME}.{TMP_TABLE} 
                    (message_key, text_json, processing_status, kafka_offset)
                    VALUES (:key, :message, 'PROCESSED', :offset)
                    """),
                    [{"key": r['key'], "message": r['message'], "offset": r['offset']} for r in records]
                )
                processed = len(records)
                
                if last_offset is not None:
                    store_last_offset(last_offset)
        
        logger.info(f"Успешно обработано {processed} сообщений. Последний offset: {last_offset}")
        return {'processed': processed, 'last_offset': last_offset}
    
    except Exception as e:
        logger.error(f"Критическая ошибка обработки: {str(e)}", exc_info=True)
        raise

@task
def setup_database():
    """Инициализирует структуру БД"""
    _ensure_tables_exist()
    logger.info("Проверка/создание структуры БД завершена")

@task
def generate_rq_uid():
    """Генерирует уникальный ID для трейсинга"""
    return str(uuid.uuid4())

@task
def transfer_to_main_table():
    """Переносит данные из временной таблицы в основную"""
    engine = _get_engine(CONN_ID_KAP)
    
    with engine.begin() as conn:
        total = conn.execute(text(f"""
            SELECT COUNT(*) 
            FROM {SCHEMA_NAME}.{TMP_TABLE}
            WHERE processing_status = 'PROCESSED'
            AND processed_at IS NULL
        """)).scalar()
        
        if total == 0:
            logger.info("Нет новых записей для переноса")
            return 0
        
        logger.info(f"Начинаем перенос {total} записей в основную таблицу")
        
        processed = 0
        while processed < total:
            chunk = min(CHUNK_SIZE, total - processed)
            inserted = conn.execute(text(f"""
                WITH batch AS (
                    SELECT id, text_json, kafka_offset
                    FROM {SCHEMA_NAME}.{TMP_TABLE}
                    WHERE processing_status = 'PROCESSED'
                    AND processed_at IS NULL
                    ORDER BY kafka_offset
                    LIMIT {chunk}
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
                RETURNING id
            """)).rowcount
            
            updated = conn.execute(text(f"""
                UPDATE {SCHEMA_NAME}.{TMP_TABLE}
                SET processed_at = CURRENT_TIMESTAMP
                WHERE id IN (
                    SELECT id 
                    FROM {SCHEMA_NAME}.{TMP_TABLE}
                    WHERE processing_status = 'PROCESSED'
                    AND processed_at IS NULL
                    ORDER BY kafka_offset
                    LIMIT {chunk}
                    FOR UPDATE SKIP LOCKED
                )
            """)).rowcount
            
            processed += updated
            logger.info(f"Перенесено {updated} записей. Всего: {processed}/{total}")
        
        return processed

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

@task
def log_processing_stats():
    """Логирует статистику обработки"""
    engine = _get_engine(CONN_ID_KAP)
    with engine.connect() as conn:
        tmp_stats = conn.execute(text(f"""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN processed_at IS NULL THEN 1 END) as pending,
                MIN(created_at) as oldest_record,
                MAX(kafka_offset) as max_offset
            FROM {SCHEMA_NAME}.{TMP_TABLE}
        """)).fetchone()
        
        main_stats = conn.execute(text(f"""
            SELECT COUNT(*), MAX(event_timestamp) 
            FROM {SCHEMA_NAME}.{MAIN_TABLE}
        """)).fetchone()
    
    logger.info(
        f"\n=== Статистика обработки ===\n"
        f"Временная таблица:\n"
        f"- Всего записей: {tmp_stats[0]}\n"
        f"- Ожидают обработки: {tmp_stats[1]}\n"
        f"- Самая старая запись: {tmp_stats[2]}\n"
        f"- Последний offset: {tmp_stats[3]}\n"
        f"Основная таблица:\n"
        f"- Всего записей: {main_stats[0]}\n"
        f"- Последнее событие: {main_stats[1]}\n"
        f"Текущий offset: {get_last_offset()}"
    )

@dag(
    start_date=datetime(2025, 4, 9),
    schedule_interval="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=4,
    dagrun_timeout=timedelta(minutes=10),
    render_template_as_native_obj=True,
    tags=['kafka', 'postgres', 'scpl', 'production', 'bulk_processing'],
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=2),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(minutes=10)
    }
)
def job_kafka_bulk_processing():
    init_db = setup_database()
    rq_uid = generate_rq_uid()
    
    # Основная задача обработки сообщений из Kafka
    process = ConsumeFromTopicOperator(
        task_id="process_messages",
        kafka_config_id="kafka_synapce",
        topics=[KAFKA_TOPIC_C],
        apply_function="kafka_utils.consume_function",
        apply_function_kwargs={
            "rqUid": "{{ ti.xcom_pull(task_ids='generate_rq_uid')}}",
            "offset_storage_key": OFFSET_STORAGE_KEY
        },
        poll_timeout=300,
        max_messages=BATCH_LIMIT,
        max_batch_size=CHUNK_SIZE
    )
    
    transfer = transfer_to_main_table()
    cleanup = cleanup_old_data()
    log_stats = log_processing_stats()
    
    # Порядок выполнения задач
    init_db >> rq_uid >> process >> transfer >> cleanup >> log_stats

job_kafka_bulk_processing()