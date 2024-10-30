FROM apache/airflow:2.5.0 

# Switch to root user to perform updates and install packages
USER root

# Update the package list and install required packages
RUN apt-get update && apt-get install -y \
    build-essential \ 
    libpq-dev         

# Switch to the airflow user to install Python packages
USER airflow

# Copy the requirements.txt file into the container
COPY requirements.txt /opt/airflow/requirements.txt

# Install the Python dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Switch back to root user to create directories and change permissions
USER root

# Copy the DAG files from the local directory to the appropriate directory in the container
COPY ./dags /opt/airflow/dags

# Copy the custom shell script into the container
COPY folder.sh /opt/airflow/folder.sh

# Ensure the shell script has executable permissions
RUN chmod +x /opt/airflow/folder.sh

# Execute the shell script to create necessary folders during the image build process
RUN /opt/airflow/folder.sh