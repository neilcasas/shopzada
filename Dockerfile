FROM apache/airflow:2.7.3
# Set working directory
ENV AIRFLOW_HOME=/opt/airflow
# Switch to airflow user
USER airflow
# Copy and install extra Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
