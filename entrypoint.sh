#!/bin/bash
set -e

# Инициализация БД Airflow
airflow db init

# Создание пользователя Airflow
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Запуск веб-сервера
exec airflow webserver