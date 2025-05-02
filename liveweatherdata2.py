
# Importing the necessary libraries needed to run our DAG
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2

default_args = {
    'owner': 'kepher',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 30),
    'email': ['mutulikepher@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

def fetch_weather_data(**kwargs):
    city_id = 184745
    api_key = "#######################"
    url = f"https://api.openweathermap.org/data/2.5/weather?id={city_id}&appid={api_key}&units=metric"

    response = requests.get(url)
    data = response.json()

    weather_info = {
        "City": data["name"],
        "Temperature (°C)": data["main"]["temp"],
        "Humidity (%)": data["main"]["humidity"],
        "Weather": data["weather"][0]["description"].title(),
        "Wind Speed (m/s)": data["wind"]["speed"]
    }

    # Push to XCom
    kwargs['ti'].xcom_push(key='weather_info', value=weather_info)
    print("✅ Weather fetched:", weather_info)

def store_weather_data(**kwargs):
    weather_info = kwargs['ti'].xcom_pull(task_ids='fetch_weather', key='weather_info')

    conn = psycopg2.connect(
        host="#######################",
        port="#######",
        dbname="#########",
        user="#########",
        password="###############"
    )
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        city TEXT,
        temperature REAL,
        humidity INTEGER,
        weather TEXT,
        wind_speed REAL,
        timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    INSERT INTO weather_data (city, temperature, humidity, weather, wind_speed)
    VALUES (%s, %s, %s, %s, %s)
    """, (
        weather_info["City"],
        weather_info["Temperature (°C)"],
        weather_info["Humidity (%)"],
        weather_info["Weather"],
        weather_info["Wind Speed (m/s)"]
    ))

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Weather data saved to PostgreSQL!")

with DAG(
    'kepher_weather_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['weather']
) as dag:

    fetch_weather = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather_data,
        provide_context=True
    )

    store_weather = PythonOperator(
        task_id='store_weather',
        python_callable=store_weather_data,
        provide_context=True
    )

    fetch_weather >> store_weather