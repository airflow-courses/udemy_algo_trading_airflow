#!/bin/sh

airflow db init

airflow scheduler \
  & exec airflow webserver --pid /tmp/airflow.pid
