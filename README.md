
# Apache Airflow + Kafka + PostgreSQL (Docker)

Данный проект предоставляет готовую инфраструктуру в виде Docker-контейнеров для запуска Apache Airflow с поддержкой Apache Kafka и PostgreSQL.

## 📁 Структура проекта

```
new_air/
├── scripts/
│   └── init_connections.py     # Создание подключений в Airflow при инициализации
├── docker-compose.yml          # Описание сервисов Docker
├── Dockerfile                  # Сборка образа Airflow
├── entrypoint.sh               # Скрипт инициализации и запуска Airflow
├── requirements.txt            # Список Python-зависимостей
└── README.md                   # Текущий файл документации
```

## ⚙️ Компоненты

Проект включает следующие контейнеризованные сервисы:

| Сервис        | Назначение |
|---------------|------------|
| **Apache Airflow** | Платформа для оркестрации ETL-процессов. Web UI доступен на порту 8080 |
| **PostgreSQL**     | Реляционная БД для хранения метаданных Airflow и пользовательских данных |
| **Kafka**          | Распределённая система потоковой обработки сообщений |
| **ZooKeeper**      | Необходим для работы Kafka — управление кластером |
| **Kafka UI**       | Веб-интерфейс управления Kafka, доступен на порту 8081 |

## 🔧 Подключения в Airflow

Создаются автоматически при первом запуске:

| ID подключения     | Тип подключения | Цель использования |
|--------------------|------------------|---------------------|
| `kafka_synapce`    | Kafka            | Подключение к брокеру Kafka |
| `kap_247_db`       | PostgreSQL       | Подключение к БД PostgreSQL |

Пример конфигурации:
```python
{
    "conn_id": "kafka_synapce",
    "conn_type": "kafka", 
    "extra": '{"bootstrap.servers": "kafka:9092"}'
}
{
    "conn_id": "kap_247_db",
    "conn_type": "postgres",
    "host": "postgres",
    "port": 5432,
    "login": "data_user",
    "password": "data_password",
    "schema": "kap_247_db"
}
```

## 🐳 Запуск проекта

1. Убедитесь, что у вас установлен [Docker](https://docs.docker.com/engine/install/) и [Docker Compose](https://docs.docker.com/compose/install/).
2. Клонируйте репозиторий:
```bash
git clone https://github.com/ваше-имя-репо/new_air.git
cd new_air
```
3. Соберите и запустите контейнеры:
```bash
docker-compose up -d --build
```

## 🌐 Доступные сервисы

| Сервис             | URL                            | Логин / Пароль         |
|--------------------|--------------------------------|------------------------|
| Airflow Web UI     | http://localhost:8080          | `admin` / `admin`      |
| Kafka UI           | http://localhost:8081          | Без авторизации        |
| PostgreSQL         | Хост: `postgres:5432`          | `data_user:data_password` |
| Kafka Brokers      | `kafka:9092`                   |                        |

## 🛠️ Дополнительные действия

### 🔄 Пересоздать метаданные Airflow

Если нужно пересоздать БД Airflow, удалите том с данными:
```bash
docker-compose down -v
docker-compose up -d --build
```

### 📦 Обновление зависимостей

Чтобы добавить или обновить зависимости, измените `requirements.txt`, затем выполните:
```bash
docker-compose build airflow
```

## ✅ Технологии

- [Apache Airflow](https://airflow.apache.org/)
- [Kafka](https://kafka.apache.org/)
- [PostgreSQL](https://www.postgresql.org/)
- [Docker & Docker Compose](https://www.docker.com/)

## 📝 Лицензия

MIT License — см. файл [LICENSE](LICENSE) для получения подробной информации.

--- 

Вы можете дополнить этот README информацией о ваших DAG-ах, если они будут храниться в этом же репозитории.
