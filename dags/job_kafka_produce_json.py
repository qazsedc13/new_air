from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer
import json
import uuid
import random
from faker import Faker

# Инициализируем Faker для генерации случайных имен
fake = Faker('ru_RU')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

def get_kafka_config():
    """Получаем конфигурацию из соединения kafka_default типа Generic"""
    conn = BaseHook.get_connection('kafka_synapce')
    extra_config = json.loads(conn.extra or '{}')
    return {
        'bootstrap.servers': f"{conn.host}:{conn.port}",
        **extra_config
    }

def generate_user_info(count):
    """Генерируем список пользователей со случайными данными"""
    users = []
    for _ in range(count):
        user = {
            "user_id": str(uuid.uuid4()),
            "tenant_user_id": str(uuid.uuid4()),
            "proficiency_level": random.randint(0, 5),
            "connection_type": random.choice(["UNDEFINED_CONNECTION_TYPE", "PHONE", "CHAT", "EMAIL", "SOCIAL"]),
            "interaction_search_tactic": random.choice(["UNDEFINED_INTERACTION_SEARCH_TACTIC", "SKILLS", "AVAILABILITY"]),
            "last_name": fake.last_name(),
            "first_name": fake.first_name(),
            "patronymic": fake.middle_name(),
            "employee_id": str(random.randint(100000, 999999))
        }
        users.append(user)
    return users

def generate_event_schema():
    """Генерируем схему события"""
    return {
        "type": "struct",
        "fields": [
            {
                "type": "string",
                "field": "event_type",
                "optional": False
            },
            {
                "type": "string",
                "field": "timestamp",
                "optional": False
            },
            {
                "type": "string",
                "field": "tenant_id",
                "optional": False
            },
            {
                "type": "string",
                "field": "division_id",
                "optional": False
            },
            {
                "type": "string",
                "field": "division_name"
            },
            {
                "type": "string",
                "field": "full_division_name"
            },
            {
                "type": "string",
                "field": "service_id",
                "optional": False
            },
            {
                "type": "string",
                "field": "service_name"
            },
            {
                "type": "string",
                "field": "message_id",
                "optional": False
            },
            {
                "type": "string",
                "field": "service_kind"
            },
            {
                "type": "array",
                "field": "user_info",
                "items": {
                    "type": "struct",
                    "fields": [
                        {
                            "type": "string",
                            "field": "user_id"
                        },
                        {
                            "type": "string",
                            "field": "last_name"
                        },
                        {
                            "type": "string",
                            "field": "first_name"
                        },
                        {
                            "type": "string",
                            "field": "patronymic"
                        },
                        {
                            "type": "string",
                            "field": "employee_id"
                        },
                        {
                            "type": "string",
                            "field": "tenant_user_id"
                        },
                        {
                            "type": "string",
                            "field": "proficiency_level"
                        },
                        {
                            "type": "string",
                            "field": "connection_type"
                        },
                        {
                            "type": "string",
                            "field": "interaction_search_tactic"
                        }
                    ],
                    "optional": False
                },
                "optional": False
            }
        ],
        "optional": False,
        "name": "bulkAgentServices_events"
    }

def produce_messages(**context):
    conf = get_kafka_config()
    producer = Producer(conf)
    topic = "SCPL.BULKAGENTSERVICESEVENT.V1"
    
    event_types = [
        "BULK_DELETE_AGENT_SERVICES",
        "BULK_ADD_AGENT_SERVICES",
        "BULK_UPDATE_AGENT_SERVICES"
    ]
    
    service_kinds = [
        "STANDARD",
        "PREMIUM",
        "CUSTOM"
    ]
    
    test_messages = []
    
    for i in range(100000):  # Генерируем 5 сообщений
        user_count = random.randint(1, 5)  # Случайное количество пользователей от 1 до 5
        
        payload = {
            "timestamp": datetime.now().isoformat(),
            "event_type": random.choice(event_types),
            "tenant_id": str(uuid.uuid4()),
            "division_id": str(uuid.uuid4()),
            "service_id": str(uuid.uuid4()),
            "message_id": str(uuid.uuid4()),
            "service_kind": random.choice(service_kinds),
            "user_info": generate_user_info(user_count),
            "service_name": f"Сервис {fake.word().capitalize()}",
            "division_name": f"SCPL {fake.company()}"
        }
        
        message = {
            "payload": payload,
            "schema": generate_event_schema()
        }
        
        test_messages.append(message)
    
    for msg in test_messages:
        producer.produce(
            topic=topic,
            key=str(msg["payload"]["message_id"]).encode('utf-8'),
            value=json.dumps(msg).encode('utf-8'),
            callback=lambda err, msg: print(f"Ошибка: {err}") if err else None
        )
        print(f"Отправлено в {topic}: {msg['payload']['message_id']}")
    
    producer.flush()

with DAG(
    dag_id='produce_agents_events',
    default_args=default_args,
    description='Отправка тестовых событий агентов в Kafka',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    tags=['kafka', 'agents'],
    catchup=False,
) as dag:

    send_messages = PythonOperator(
        task_id='send_agents_events',
        python_callable=produce_messages,
    )