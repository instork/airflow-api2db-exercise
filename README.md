# airflow-api2db-exercise

## Prerequisites
- Install docker
- Install docker-compose
- Create docker/airflow-pymongo/mongo.env like below
    ```
    MONGODB_USER=airflow
    MONGODB_PWD=airflow
    MONGODB_HOST=mongoservice
    MONGODB_PORT=27017
    ```
- Create docker/airflow-pymongo/fred.env like below
    ```
    FRED_API_KEY=<FRED_API_KEY>
    ```


&nbsp;

## How to run
- run docker-compose
    ```bash
    $ docker-compose up
    $ docker-compose up --build --remove-orphans --force-recreate
    $ docker-compose up --build --remove-orphans --force-recreate --detach
    ```

- stop docker-compose
    ```bash
    $ docker-compose down
    $ docker-compose down --volumes --remove-orphans
    ```

&nbsp;

## Data Time
- Data Request Time (New York Time)
    - Upbit Data : every hour from 00:00 GMT-4
    - Google News : every day from 00:00 GMT-4
    - Fred Data : every day from 00:00 GMT-4 (might be missing on weekend & holidays)

&nbsp;

## Check data
``` bash
$ docker ps -a --no-trunc --filter name=mongo 
$ docker exec -it <CONTAINER ID> /bin/bash    
$ mongo -u airflow -p airflow
$ show dbs
$ use test_db
$ show collections
$ db["<collection_name>"].find()
$ db.dropDatabase()
```

&nbsp;

## (Optional) Prerequisites for MongoDB on local
- Make mongodb user
    - https://medium.com/mongoaudit/how-to-enable-authentication-on-mongodb-b9e8a924efac
- Connecting from a Docker container to a local MongoDB
    - https://tsmx.net/docker-local-mongodb/
- Make docker/airflow-pymongo/.env file on like [.env sample](https://github.com/instork/airflow-api2db-exercise/blob/main/docker/airflow-pymongo/.env_example)
    - change MONGODB_USER, MONGODB_PWD
- Use docker-compose-localdb.yaml as docker-compose.yaml