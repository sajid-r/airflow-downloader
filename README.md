# Downloading files using Airflow

## Reference
https://github.com/puckel/docker-airflow.git

## Setup
### Build docker image
```
docker build --rm  -t docker-airflow:latest .
```
### To develop in local machine
```
docker run -d -p 8080:8080 docker-airflow:latest webserver
```
### URLs to access for development
#### Airflow
```
http://localhost:8080/airflow/admin/
```
## Dependencies for code
```
pip install -r requirements.txt
```