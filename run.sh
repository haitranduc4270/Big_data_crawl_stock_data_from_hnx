#!/bin/bash

docker compose up --build -d
docker compose down
# docker exec -it crawler pip install pyspark
# docker exec -it pyspark-application pip install pyspark

elasticdump \
  --input= 19_1_mapping.json \
  --output=http://localhost:9200/stock_data_realtime_19_1 \
  --type=mapping


elasticdump \
  --input=http://localhost:9200/stock_data_realtime_19_1 \
  --output=19_1_data.json \
  --type=data