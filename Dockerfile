FROM apache/airflow:2.0.0

USER root

# Install necessary packages
RUN apt-get update --fix-missing && \
    apt-get install -y build-essential default-libmysqlclient-dev libpq-dev

USER airflow

# Install Python packages
RUN pip install --no-cache-dir polars
RUN pip install --no-cache-dir beautifulsoup4 pandas sqlalchemy psycopg2-binary

# Upgrade typing-extensions
RUN pip install typing-extensions --upgrade
