# Лабораторная работа 2

## Содержание

* [Выбор инструментов](#выбор-инструментов)
* [Установка и развертывание](#установка-и-развертывание)
* [Ссылки на использованные материалы](#ссылки-на-использованные-материалы)

## Выбор инструментов

Для работы с Spark был выбран язык программирования python и библиотека предоставляющая необходимое API - PySpark.

Выполненные задания назодятся в .ipynb файле.

## Установка и развертывание
1. Развернуть контейнер (https://hub.docker.com/r/maprtech/dev-sandbox-container)

        sudo rm -rf /tmp/maprdemo

        sudo mkdir -p /tmp/maprdemo/hive /tmp/maprdemo/zkdata /tmp/maprdemo/pid /tmp/maprdemo/logs /tmp/maprdemo/nfs
        sudo chmod -R 777 /tmp/maprdemo/hive /tmp/maprdemo/zkdata /tmp/maprdemo/pid /tmp/maprdemo/logs /tmp/maprdemo/nfs

        export clusterName="maprdemo.mapr.io"
        export MAPR_EXTERNAL='0.0.0.0'
        export PORTS='-p 9998:9998 -p 8042:8042 -p 8888:8888 -p 8088:8088 -p 9997:9997 -p 10001:10001 -p 8190:8190 -p 8243:8243 -p 2222:22 -p 4040:4040 -p 7221:7221 -p 8090:8090 -p 5660:5660 -p 8443:8443 -p 19888:19888 -p 50060:50060 -p 18080:18080 -p 8032:8032 -p 14000:14000 -p 19890:19890 -p 10000:10000 -p 11443:11443 -p 12000:12000 -p 8081:8081 -p 8002:8002 -p 8080:8080 -p 31010:31010 -p 8044:8044 -p 8047:8047 -p 11000:11000 -p 2049:2049 -p 8188:8188 -p 7077:7077 -p 7222:7222 -p 5181:5181 -p 5661:5661 -p 5692:5692 -p 5724:5724 -p 5756:5756 -p 10020:10020 -p 50000-50050:50000-50050 -p 9001:9001 -p 5693:5693 -p 9002:9002 -p 31011:31011'

        docker run --name maprdemo -d --privileged -v /tmp/maprdemo/zkdata:/opt/mapr/zkdata -v /tmp/maprdemo/pid:/opt/mapr/pid  -v /tmp/maprdemo/logs:/opt/mapr/logs  -v /tmp/maprdemo/nfs:/mapr $PORTS -e MAPR_EXTERNAL -e clusterName -e isSecure --hostname ${clusterName} maprtech/dev-sandbox-container:latest > /dev/null 2>&1

        sleep 400

        docker exec -d maprdemo usermod -s /bin/bash root
        docker exec -d maprdemo usermod -s /bin/bash mapr
        docker exec -d maprdemo apt install -y mapr-resourcemanager mapr-nodemanager mapr-historyserver
        docker exec -d maprdemo /opt/mapr/server/configure.sh -R

2. Подключение по ssh

        ssh root@localhost -p 2222

    пароль mapr

3. Добавление spark в окружение

        echo 'export PATH=$PATH:/opt/mapr/spark/spark-2.4.5/bin' >  home/mapr/.bash_profile

4. Установка python

        sudo apt update
        sudo apt install python
        sudo apt install python3-pip

5. Установка необходимых библиотек

        pip3 install jupyter
        pip3 install pyspark

6. Запуск jupyter (из заранее созданой директории)

        jupyter-notebook --ip=0.0.0.0 --port=50001 --allow-root --no-browser

    --ip и --port необходимы, чтобы подключится к серверу jupyter из хоста

    --allow-root запускает сервер от имени root пользователя

    --no-browser не запускает браузер (т.к. мы в контейнере)

7. В интерфейсе jupyter создаем новый ноутбук

## Пояснения к заданию находятся в jupter ноутбуке
## Ссылки на использованные материалы
* https://git.ai.ssau.ru/tk/big_data/src/branch/master/L2%20-%20Reports%20with%20Apache%20Spark
* https://sparkbyexamples.com/spark/spark-sql-functions/
* https://stackoverflow.com/questions/33224740/best-way-to-get-the-max-value-in-a-spark-dataframe-column
* https://stackoverflow.com/questions/39858238/how-can-i-save-rdd-to-a-single-parquet-file
* https://stackoverflow.com/questions/38397796/retrieve-top-n-in-each-group-of-a-dataframe-in-pyspark