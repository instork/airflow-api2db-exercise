# airflow-api2db-exercise

## Prerequisites
- install docker
- install docker-compose

- run docker-compose
```
$ docker-compose up
$ docker-compose up --build --remove-orphans --force-recreate
```
- stop docker-compose
```
$ docker-compose down --volumes --remove-orphans
```

- mongodb
    - https://tsmx.net/docker-local-mongodb/
    ```
    $ docker network inspect bridge 
    ```
    ```
    $ sudo service mongod restart    
    ```