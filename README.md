# Air Pollution Data Analytics Pipeline

This is a project that implements an end-to-end data analytics pipeline. Data is extracted from an API and a transformed dataset with geocode containing the top 60 populated cities in the world. The range is limited to 60 countries to comply with the free account rate limit of the API. The pipeline is configured to run every 8:00 UTC, but it can be configured to run at any schedule and interval however it depends on the limitation of services required.

![Air Pollution Data Analytics Pipeline](https://github.com/zvsuarez/air-pollution-data-pipeline/assets/64736073/d11c24e6-9221-4fdc-98aa-954cc52cebfb)

## Architecture

### 1. Data Source (OpenWeather + Simplemaps)

Data sources include the Openweather API and Simplemaps dataset. The API call provides the present air pollution data, while the dataset from Simplemaps is transformed to only store the top 60 most populated cities with its geocode information. The coordinates from the dataset is plugged into the API call to retrieve the pollution data.

### 2. Orchestration (Airflow/Amazon EC2)

Airflow is deployed on an EC2 instance. DAG scripts, configuration, and credential files reside inside the virtual machine. Dependencies are also installed in the instance. Airflow is scheduled to run with catchup enabled.

### 3. ETL (Python/Pandas -> Amazon S3 -> Amazon Redshift)

Python scripts contain either the main extract & transform process, or the DAG which contains the `etl` and `load` task. After extracting and transforming the data it is ingested to S3, which is subsequently checked by a `load` task. The load task checks S3 for the latest object stored using an access point, and copies it into a Redshift serverless table which serves as the sink.

### 4. Dashboard

Tableau is connected to the table in Redshift, and analytics is configured based on several questions to be analysed. These contain the list of countries ranked by `population`, `air quality`, different `parameter` levels, and graphs.

To know what parameters the API provide, refer to this: [Air Pollution API](https://openweathermap.org/api/air-pollution).

Note: As of this moment, data accumulation is in progress and the dashboard is not yet finished. The public URL of the dash will be posted here.

### 5. IAM (Identity Access Management)

Roles attached to services vary based on their interactions with other components. AWS managed keys and custom managed keys were both utilised which depend on how flexible the requirements are. Roles attached to the EC2 instance have permissions in S3 and Redshift Data. Redshift Serverless workgroup also have permissions in S3, and a customer managed policy `Redshift Commands` for allowing to run SQL commands to copy, load/unload, query and analyse data. Service roles are automatically available.
