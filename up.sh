cd articles_crawler
npm i
cd ..
cd articles_server
npm i
cd ..
sudo docker compose up -d
sudo docker exec namenode  hdfs dfsadmin -safemode leave