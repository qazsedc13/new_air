#!/bin/bash
set -e

# Инициализация БД Airflow
if [ ! -f "${AIRFLOW_HOME}/airflow.db" ]; then
    airflow db init
    
    airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com
        
    # Инициализация подключений
    python /opt/airflow/scripts/init_connections.py
fi

# Запуск веб-сервера
exec airflow webserver