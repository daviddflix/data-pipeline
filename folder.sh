#!/bin/bash
mkdir -p /opt/airflow/dags/files/webscraper 
mkdir -p /opt/airflow/dags/files/preprocessed
chmod -R 777 /opt/airflow/dags/files
mkdir -p /opt/airflow/logs
mkdir -p /opt/airflow/plugins
chmod -R 777 /opt/airflow/logs
chmod -R 777 /opt/airflow/plugins