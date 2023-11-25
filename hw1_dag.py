from datetime import datetime
from datetime import timezone
import logging
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

from airflow.models import Variable

CITY_COORDS = {
        "Lviv": [49.839684, 24.029716],
        "Kyiv": [50.447731, 30.542721],
        "Odesa": [46.476608, 30.707310],
        "Kharkiv": [49.993500, 36.230385],
        "Zhmerynka": [49.037560, 28.108900]
}

def _process_weather(ti, **kwargs):
    info = ti.xcom_pull(f"extract_data_{kwargs['city']}")["data"][0]
    timestamp = info["dt"]
    temp = info["temp"]
    hum = info["humidity"]
    cloudiness = info["clouds"]
    wind_speed = info["wind_speed"]
    return timestamp, temp, hum, cloudiness, wind_speed

def _get_dt(ti, ds):
    timestamp = int(
        datetime.strptime(ds, "%Y-%m-%d")
        .replace(tzinfo=timezone.utc)
        .timestamp()
    )
    return str(timestamp)

with DAG(dag_id="hw1_dag", schedule_interval="@daily", start_date=datetime(2023, 11, 18), catchup=True) as dag:
    b_create = SqliteOperator(
        task_id="create_table_sqlite",
        sqlite_conn_id="airflow_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS measures
        (
        city TEXT,
        timestamp TIMESTAMP,
        temp FLOAT,
        humidity INT,
        cloudiness TEXT,
        wind_speed FLOAT
        );"""
    )

    for city in CITY_COORDS:

        lat = CITY_COORDS[city][0]
        lon = CITY_COORDS[city][1]

        check_api = HttpSensor(
            task_id=f"check_api_{city}",
            http_conn_id="weather_conn",
            endpoint="data/3.0/onecall",
            request_params={
                "appid": Variable.get("WEATHER_API_KEY"),
                "lat": str(lat),
                "lon": str(lon)
            }
        )

        # Needed so that no city halts and results in incorrect dt data 
        get_dt = PythonOperator(
            task_id=f"get_dt_{city}", python_callable=_get_dt
        )

        extract_data = SimpleHttpOperator(
            task_id=f"extract_data_{city}",
            http_conn_id="weather_conn",
            endpoint="data/3.0/onecall/timemachine",
            data={
                "appid": Variable.get("WEATHER_API_KEY"),
                "lat": str(lat),
                "lon": str(lon),
                "dt": f"""{{{{ti.xcom_pull(task_ids='get_dt_{city}')}}}}"""
            },
            method="GET",
            response_filter=lambda x: json.loads(x.text),
            log_response=True
        )

        
        process_data = PythonOperator(
            task_id=f"process_data_{city}",
            python_callable=_process_weather,
            op_kwargs={'city': city},
        )

        inject_data = SqliteOperator(
            task_id=f"inject_data_{city}",
            sqlite_conn_id="airflow_conn",
            sql="""
            INSERT INTO measures (city, timestamp, temp, humidity, cloudiness, wind_speed) VALUES
            (
                '%s',
                {{ti.xcom_pull(task_ids='process_data_%s')[0]}},
                {{ti.xcom_pull(task_ids='process_data_%s')[1]}},
                {{ti.xcom_pull(task_ids='process_data_%s')[2]}},
                '{{ti.xcom_pull(task_ids='process_data_%s')[3]}}',
                {{ti.xcom_pull(task_ids='process_data_%s')[4]}}
            );
            """ % (city, city, city, city, city, city),
        )

        b_create >> check_api >> get_dt >> extract_data >> process_data >> inject_data
