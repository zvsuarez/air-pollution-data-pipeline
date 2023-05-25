#!/bin/bash

sudo apt-get update
sudo apt-get upgrade
sudo apt install python3
sudo apt install python3-pip
sudo pip install apache-airflow
sudo pip install apache-airflow[amazon]
sudo pip install apache-airflow-providers-amazon
sudo pip install requests
sudo pip install pandas
sudo pip install boto3
#sudo apt-get install libpq-dev python3-dev
#sudo pip install psycopg2
