FROM python:3.8-slim
LABEL maintainer="Puckel_"

# Never prompt the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=2.0.1
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ARG PYTHON_VERSION=3.8
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.8.txt"

# Define ru_RU.
ENV LANGUAGE ru_RU.UTF-8
ENV LANG ru_RU.UTF-8
ENV LC_ALL ru_RU.UTF-8
ENV LC_CTYPE ru_RU.UTF-8
ENV LC_MESSAGES ru_RU.UTF-8

# Disable noisy "Handling signal" log messages:
# ENV GUNICORN_CMD_ARGS --log-level WARNING

ARG BUILD_DEPS=" \
    freetds-dev \
    libkrb5-dev \
    libsasl2-dev \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    git \
    unixodbc-dev \
"

RUN set -ex \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        ${BUILD_DEPS} \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
        gnupg2 \
    && sed -i 's/^# ru_RU.UTF-8 UTF-8$/ru_RU.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=ru_RU.UTF-8 LC_ALL=ru_RU.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow \
    && pip install -U pip setuptools wheel \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyodbc \
    && pip install tinvest \
    && pip install psycopg2 \
    && pip install pyasn1 \
    && pip install pymssql \
    && pip install mysqlclient \
    && pip install xmltodict \
    && pip install ldap3 \
    && pip install apache-airflow[crypto,celery,postgres,vertica,odbc,password]==${AIRFLOW_VERSION} --constraint "${CONSTRAINT_URL}" \
    && pip install 'apache-airflow-providers-papermill' \
    && pip install 'apache-airflow-providers-postgres' \
    && pip install 'apache-airflow-providers-vertica' \
    && apt install gcc -y \
    && apt install default-libmysqlclient-dev -y \
    && pip install 'apache-airflow-providers-mysql' \
    && pip install 'apache-airflow-providers-microsoft-mssql' \
    && pip install redis \
    && pip install flask-bcrypt \
    && pip install papermill \
    && pip install jupyter \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi \
    && apt-get purge --auto-remove -yqq ${BUILD_DEPS} \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base


RUN mkdir -p /var/log/airflow /var/spool/airflow \
    && chown -R airflow: ${AIRFLOW_USER_HOME} \
    && chown -R airflow: /var/log/airflow \
    && chown -R airflow: /var/spool/airflow

RUN mkdir -p /var/notebooks/output \
    && chown -R airflow: /var/notebooks/output

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["./entrypoint.sh"]
