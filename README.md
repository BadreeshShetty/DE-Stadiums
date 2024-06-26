# Data Analytics Engineering Project: Stadium Data ETL using Docker, Airflow, AWS, BeautifulSoup, Snowflake, and Tableau

This project involves setting up a data pipeline for scraping, transforming, and loading stadium data using Docker, Airflow, AWS (S3), BeautifulSoup, Snowflake, and Tableau with Python and SQL.

## 📁 **Description**

The main objective of this project is to gather and process stadium data, leveraging various tools and technologies to manage and process the information. This project showcases my expertise in data engineering and my ability to handle complex data workflows.

---
## Initial Link to Scrape

Link: https://en.wikipedia.org/wiki/List_of_stadiums_by_capacity

<img width="1470" alt="DAE-ETL-scrape" src="https://github.com/BadreeshShetty/DE-Stadiums/blob/main/Stadiums-List-Scrape.png">


## End Result Table in Snowflake

<img width="1470" alt="DAE-ETL-table" src="https://github.com/BadreeshShetty/DE-Stadiums/blob/main/Stadium-Snowflake-Table.png">

---

## 📹 Video Demonstration of the Project

[<img width="1470" alt="DAE-ETL" src="https://github.com/BadreeshShetty/DE-Stadiums/blob/main/DAE-Stadiums.png">](https://www.youtube.com/watch?v=woFmyflvnng))

Video Link: https://youtu.be/lFwdFiiomzU

---

## 🐳 **Docker (Containerization Tool)**

### Setup

- **Airflow Webserver**
- **Scheduler**
- **Postgres Image Containers**

---

## 🌬️ **Airflow (Workflow Management Tool)**

### DAGs

#### 1. Schema Table DAG

- **File**: `stadium_schema_creation_dag.py`
- **Function**: Run schema creation for staging tables.

#### 2. Scraping, ETL, Database Insertion DAG

- **File**: `wikipedia_stadium_data_pipeline_dag.py`
- **Function**: Scrape data, perform ETL, and insert into the database.

---

## 🥣 **BeautifulSoup (Web Scraping Stadium Data)**

### Tasks

- **Run Scraping**

---

## ☁️ **AWS (Data Storage)**

### S3

- **Task**: Insert data into S3 bucket.

---

## ❄️ **Snowflake (Data Warehouse)**

### Tasks

- Store and manage data inserted.

---

## 📊 **Tableau (Dashboard)**

### Tasks

- Create visualizations and dashboards.

---

### Setup Connections

### Add AWS Credentials to Docker Compose yml file

### Airflow UI Connections

- **Connection ID**: `snowflake_conn_stadium`
- **Type**: Snowflake
- **Description**: Snowflake Airflow Connection - Stadium
- **Schema**: `STADIUM_SCHEMA`
- **Login**: `*********`
- **Password**: (Set your password)
- **Account**: `****.us-east-2.aws`
- **Warehouse**: `STADIUM_WAREHOUSE`
- **Database**: `STADIUM_DB`
- **Role**: `ACCOUNTADMIN`

---

## 📌 Note

- Keep S3 policy and staging policy different. Use `stadiums` in S3 and `stadium` in code to avoid confusion.

---

## ⚠️ Challenges

- Issues with DBT image container.

---

## Installation and Setup

### Prerequisites

- Docker
- Python 3.x
- pip (Python package installer)
- AWS account
- Snowflake account
- Tableau Desktop account

### Steps

1. **Clone the repository**:
    ```sh
    git clone https://github.com/BadreeshShetty/DE-Stadiums
    cd your-repo-name
    ```

2. **Install dependencies**:
    ```sh
    pip install -r requirements.txt
    ```

3. **Set up Docker**:
    - Ensure Docker is installed and running on your machine.

4. **Configure AWS credentials**:
    Update the AWS credentials in the Docker Compose yml file.

5. **Run Docker Compose**:
    ```sh
    docker-compose up
    ```

6. **Run Airflow**:
    Set up and start the Airflow scheduler and webserver.
    ```sh
    airflow db init
    airflow scheduler
    airflow webserver
    ```

7. **Configure Snowflake connection**:
    Update the Snowflake credentials in Airflow.

8. **Run the DAGs**:
    Trigger the DAGs from the Airflow web interface.

## Project Documentation

Detailed documentation of the project can be found [Stadiums Data Analytics Engineering Project (Docker, Airflow, AWS, Web Scraping, Python, SQL, Snowflake, Tableau)](https://www.notion.so/Stadium-Data-Analytics-Engineering-Project-Docker-Airflow-AWS-Web-Scraping-Python-SQL-Snowfla-fefe58f90cd4470cbc97113873df80c8)

---

## Contact

For any questions or suggestions, feel free to reach out to me at badreeshshetty@gmail.com.
