from airflow.models import Connection
from airflow.settings import Session

connections = [
    {
        "conn_id": "kafka_synapce",
        "conn_type": "kafka", 
        "extra": '{"bootstrap.servers": "kafka:9092"}'
    },
    {
        "conn_id": "kap_247_db",
        "conn_type": "postgres",
        "host": "postgres",
        "port": 5432,
        "login": "data_user",
        "password": "data_password",
        "schema": "kap_247_db"
    }
]

session = Session()
for conn in connections:
    if not session.query(Connection).filter_by(conn_id=conn["conn_id"]).first():
        session.add(Connection(**conn))
session.commit()
session.close()