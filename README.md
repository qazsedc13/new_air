# Airflow + Kafka Environment

![License](https://img.shields.io/github/license/qazsedc13/new_air)
![Airflow Version](https://img.shields.io/badge/Airflow-2.x-blue)
![Kafka Version](https://img.shields.io/badge/Kafka-Confluent%207.2.1-green)
![Docker](https://img.shields.io/badge/Docker-Compose-green)

Современная среда для разработки конвейеров данных с интеграцией Apache Airflow и стека Apache Kafka. Проект предоставляет полнофункциональное окружение для работы с потоковыми данными: оркестрация, брокер сообщений Kafka с Zookeeper и Kafka UI для мониторинга.

---

## 🚀 Основные возможности

- **Оркестрация потоковых данных**: Автоматизация ETL-задач через Apache Airflow.
- **Стриминг в реальном времени**: Интеграция с Apache Kafka для передачи событий.
- **Мониторинг топиков**: Kafka UI для просмотра и анализа сообщений.
- **Генерация синтетических данных**: DAG-файлы с использованием библиотеки Faker.
- **Автоматическая настройка подключений**: Скрипт инициализации Airflow Connections.

---

## 🛠 Технологический стек

- **Apache Airflow 2.x**: Оркестратор задач.
- **Apache Kafka (Confluent Platform 7.2.1)**: Брокер сообщений.
- **Zookeeper**: Координация кластера Kafka.
- **Kafka UI**: Графический интерфейс для мониторинга.
- **PostgreSQL 13.8**: База данных метаданных Airflow.
- **Python 3.x**: Среда исполнения для DAG.

### Ключевые зависимости (Python)
- `confluent-kafka`
- `faker`
- `psycopg2-binary`

---

## 📋 Предварительные требования

- **Docker Engine**: 20.10+
- **Docker Compose**: 2.0+
- **RAM**: Минимум 4GB (рекомендуется 8GB для комфортной работы всего стека).
- **ОС**: Linux, macOS или Windows с WSL2.
- **Порты**: 8080, 8082, 29092, 5432 должны быть свободны.

---

## 🚀 Установка и запуск

### 1. Клонирование репозитория
```bash
git clone git@github.com:qazsedc13/new_air.git
cd new_air
```

### 2. Сборка и запуск
Запустите все сервисы (Airflow, Kafka, Zookeeper, Kafka UI, PostgreSQL):
```bash
docker compose up -d --build
```

### 3. Доступ к интерфейсам
| Сервис | URL | Логин / Пароль |
| :--- | :--- | :--- |
| **Airflow Webserver** | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` |
| **Kafka UI** | [http://localhost:8082](http://localhost:8082) | — |
| **PostgreSQL** | `localhost:5432` | — |

---

## 💡 Работа с проектом

### Генерация и отправка данных в Kafka
DAG `job_kafka_produce_json` создаёт случайные события пользователей (имена, ID, уровни квалификации) с помощью Faker и отправляет их в Kafka в формате JSON.

### Чтение и обработка данных
DAG-файлы `job_kafka_read_json4.py` и `job_kafka_read_json5.py` демонстрируют различные подходы к потреблению и обработке JSON-сообщений из топиков.

### Настройка подключений Airflow
Скрипт `scripts/init_connections.py` автоматизирует создание Connection в Airflow для взаимодействия с Kafka:
```bash
python scripts/init_connections.py
```

### Разработка DAG
Помещайте ваши Python-скрипты в директорию `./dags`. Airflow автоматически обнаружит их при запуске.

---

## 📁 Структура проекта

| Файл / Директория | Описание |
| :--- | :--- |
| `dags/` | Исходный код DAG-файлов |
| `scripts/` | Вспомогательные скрипты для настройки окружения |
| `docker-compose.yml` | Конфигурация инфраструктуры (7 сервисов) |
| `Dockerfile` | Кастомный образ Airflow с зависимостями |
| `init.sql` | SQL-скрипт инициализации БД |
| `requirements.txt` | Список Python-пакетов |
| `entrypoint.sh` | Сценарий запуска контейнера Airflow |

---

## 🔧 Управление инфраструктурой

**Остановка всех сервисов:**
```bash
docker compose down
```

**Просмотр логов:**
```bash
docker compose logs -f
```

**Полный сброс данных:**
```bash
docker compose down -v
```

**Перезапуск конкретных сервисов:**
```bash
docker compose restart airflow-webserver airflow-scheduler
```

---

## ⚠️ Устранение неполадок

**Kafka UI не подключается к кластеру:**
Убедитесь, что контейнеры `kafka` и `zookeeper` находятся в состоянии `healthy`. Проверить статус можно командой `docker compose ps`.

**Ошибка инициализации Airflow:**
Если база данных метаданных не успела подняться, планировщик может завершиться с ошибкой. Выполните перезапуск:
```bash
docker compose restart airflow-webserver airflow-scheduler
```

**Недостаточно памяти:**
При нехватке RAM (менее 4GB) некоторые сервисы могут завершаться аварийно. Освободите память или увеличьте лимит Docker.

---

## 📄 Лицензия

Проект распространяется под лицензией **MIT**.

---

## 📬 Контакты

- **Автор**: [qazsedc13](https://github.com/qazsedc13)
- **Репозиторий**: [new_air](https://github.com/qazsedc13/new_air)
