FROM apache/airflow:2.5.0

# Switch to root user to perform updates and install packages
USER root

# Update package list and install required packages
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev

# Create the required directories with proper ownership
RUN mkdir -p /opt/airflow/dags/files/webscraper \
    && mkdir -p /opt/airflow/dags/files/preprocessed \
    && chown -R airflow:root /opt/airflow/dags/files \
    && chmod -R 777 /opt/airflow/dags/files

# Copy the requirements.txt file
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

# Switch to airflow user to install Python dependencies
USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Switch back to root to copy DAGs
USER root
COPY --chown=airflow:root ./dags /opt/airflow/dags/

# Final permissions check and setup
RUN chmod -R 777 /opt/airflow/dags/files

# Switch back to airflow user for running the container
USER airflow