#!/bin/bash

docker compose up --build -d
docker compose down
# docker exec -it crawler pip install pyspark
# docker exec -it pyspark-application pip install pyspark