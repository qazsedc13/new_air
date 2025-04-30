-- Создаем базу данных для Airflow (уже создана)
-- CREATE DATABASE airflow;

-- Создаем базу данных для приложения
CREATE DATABASE kap_247_db;

-- Создаем пользователя для работы с данными
CREATE USER data_user WITH PASSWORD 'data_password';

-- Даем права новому пользователю
GRANT ALL PRIVILEGES ON DATABASE kap_247_db TO data_user;