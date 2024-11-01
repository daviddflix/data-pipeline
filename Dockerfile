# Start from the base Airflow image
FROM apache/airflow:2.5.0

# Switch to root user for system-level operations
USER root

# Update and install system dependencies
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create necessary directories with proper permissions
RUN mkdir -p /opt/airflow/dags/files/webscraper && \
    mkdir -p /opt/airflow/dags/files/preprocessed && \
    chown -R airflow:root /opt/airflow/dags/files && \
    chmod -R 775 /opt/airflow/dags/files

# Copy requirements file
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

# Switch to airflow user for Python package installation
USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy DAG files with proper ownership
USER root
COPY --chown=airflow:root ./dags /opt/airflow/dags/

# Final permission adjustments
RUN chmod -R g+rwx /opt/airflow/dags/files

# Switch back to airflow user for running the container
USER airflow