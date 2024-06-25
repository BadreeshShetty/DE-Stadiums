import logging
import os
import time
from datetime import datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup
from geopy import Nominatim
from io import StringIO
import boto3
import re

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv

load_dotenv()

AWS_KEY_ID = os.getenv('AWS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')


NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'
S3_BUCKET = 'stadiums-bucket'
SNOWFLAKE_STAGE = 'SNOW_S3_STAGE'
SNOWFLAKE_WAREHOUSE = 'STADIUM_WAREHOUSE'
SNOWFLAKE_DB = 'STADIUM_DB'
SNOWFLAKE_SCHEMA = 'STADIUM_SCHEMA'
DATA_FOLDER = 'data'


# Ensure the data folder exists
os.makedirs(DATA_FOLDER, exist_ok=True)

# Define default arguments
default_args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Initialize the DAG
dag = DAG(
    dag_id='wikipedia_stadium_data_pipeline_dag',
    default_args=default_args,
    description='Scrape Stadium Data from Wikipedia and load into Snowflake',
    schedule_interval='@daily',
    catchup=False,
)

def get_wikipedia_page(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logging.error(f"An error occurred: {e}")
        return None

def get_wikipedia_data(html):
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all("table", {"class": "wikitable"})
    all_data = []

    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['th', 'td'])
            row_data = []
            for idx, cell in enumerate(cells):
                cell_text = cell.get_text(strip=True)
                if idx != 3:
                    img = cell.find('img')
                    if img:
                        img_url = img['src']
                        if img_url.startswith("//"):
                            img_url = "https:" + img_url
                        cell_text += f"{img_url}"
                row_data.append(cell_text)
            all_data.append(row_data)
    return all_data

def clean_text(text):
    text = re.sub(r'better\xa0source\xa0needed', '', text)
    text = re.sub(r'\[\d+\]', '', text)
    text = re.sub(r'[^\w\s,-]', '', text)
    text = text.strip()
    text = text.title()
    return text

def get_lat_long(country, location):
    geolocator = Nominatim(user_agent="stadium_geopy")
    time.sleep(1)
    location = geolocator.geocode(f"{location}, {country}")
    return (location.latitude, location.longitude) if location else (None, None)

def update_locations(df):
    locations = []
    for i, (index, row) in enumerate(df.iterrows()):
        print(i)
        try:
            location = get_lat_long(row['Country'], row['Stadium'])
            locations.append(location)
        except Exception as e:
            logging.error(f"Error getting location for {row['Stadium']}, {row['Country']}: {e}")
            locations.append((None, None))
        print(i)
    df['Location'] = locations

    duplicates = df[df.duplicated(['Location'])]
    duplicate_locations = []
    for index, row in duplicates.iterrows():
        try:
            location = get_lat_long(row['Country'], row['City_State'])
            duplicate_locations.append(location)
        except Exception as e:
            logging.error(f"Error getting duplicate location for {row['City_State']}, {row['Country']}: {e}")
            duplicate_locations.append((None, None))
    duplicates['Location'] = duplicate_locations
    df.update(duplicates)

    return df


def upload_to_s3(file_path, s3_key):
    s3 = boto3.client('s3')
    s3.upload_file(file_path, S3_BUCKET, s3_key)
    return f's3://{S3_BUCKET}/{s3_key}'

def extract_load_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    if html:
        data = get_wikipedia_data(html)
        df = pd.DataFrame(data)
        df.columns = ['Stadium', 'Capacity', 'City_State', 'Country', 'Region', 'Tenants', 'Sports', 'Image']
        columns_clean = ['Stadium', 'Capacity', 'City_State', 'Country', 'Region', 'Tenants', 'Sports']
        df[columns_clean] = df[columns_clean].map(lambda x: clean_text(x))
        df = df[df['Capacity'].str.lower() != 'capacity']
        df['Capacity'] = df['Capacity'].str.replace(',', '').str.replace('.', '').astype(int)
        df['Image'] = df['Image'].apply(lambda x: NO_IMAGE if x is None else x)
        df = update_locations(df)
        
        file_path = os.path.join(DATA_FOLDER, 'stadium_data.parquet')
        df.to_parquet(file_path)
        
        s3_key = kwargs['key']
        upload_to_s3(file_path, s3_key)
        
        # kwargs['ti'].xcom_push(key='stadium_data', value=df.to_json())
    else:
        logging.error("Failed to retrieve the Wikipedia page.")

with dag:
    extract_load_data_task = PythonOperator(
        task_id="extract_load_data",
        python_callable=extract_load_wikipedia_data,
        provide_context=True,
        op_kwargs={
            "url": "https://en.wikipedia.org/wiki/List_of_stadiums_by_capacity",
            "key": "stadium_data.parquet"
        },
    )

    snowflake_copy_data = SQLExecuteQueryOperator(
        task_id='snowflake_copy_data',
        sql=f"""
                USE WAREHOUSE {SNOWFLAKE_WAREHOUSE};
        USE {SNOWFLAKE_DB};
        COPY INTO {SNOWFLAKE_SCHEMA}.stadium_data
        FROM @{SNOWFLAKE_STAGE}/stadium_data.parquet
        FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT')
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
        """,
        conn_id='snowflake_conn_stadium',
    )

    extract_load_data_task >> snowflake_copy_data
