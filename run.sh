#!/bin/bash

docker compose up -d

elasticdump \
  --input=http://localhost:9200/stock_data_realtime \
  --output=31_1_mapping.json \
  --type=mapping

elasticdump \
  --input=http://localhost:9200/stock_data_realtime \
  --output=31_1_data.json \
  --type=data
