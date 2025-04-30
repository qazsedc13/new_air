FROM python:3.9-slim-bullseye

# Установка системных зависимостей
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Создание пользователя airflow
RUN useradd -m -U -d /home/airflow -s /bin/bash airflow

# Рабочая директория
WORKDIR /home/airflow
ENV AIRFLOW_HOME=/home/airflow/airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"

# Копируем зависимости
COPY --chown=airflow:airflow requirements.txt .

# Установка Python-зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Копируем остальные файлы
COPY --chown=airflow:airflow . .

# Инициализация Airflow
RUN mkdir -p ${AIRFLOW_HOME} && \
    chown -R airflow:airflow ${AIRFLOW_HOME}

# Порт для веб-сервера
EXPOSE 8080

# Entrypoint скрипт
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow
ENTRYPOINT ["/entrypoint.sh"]