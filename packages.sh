#!/bin/bash

sudo apt-get update
sudo apt-get upgrade
sudo apt install python
sudo pip install apache-airflow
sudo pip install apache-airflow[amazon]
sudo pip install apache-airflow-providers-amazon
sudo pip install requests
sudo pip install pandas
sudo pip install psycopg2