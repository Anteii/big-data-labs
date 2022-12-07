# Лабораторная работа 3

## Содержание

* [Выбор инструментов](#выбор-инструментов)
* [Установка и развертывание](#установка-и-развертывание)
* [Тесты](#тесты)
* [Ссылки на использованные материалы](#ссылки-на-использованные-материалы)

## Выбор инструментов

Для данной работы использовались следующие инстурменты
* open_jdk 1.8.0_42
* IntelliJ IDEA 2021.3.3
* Apache Flink 1.10.0

Задания были реализованы на языке Java

## Установка и развертование

1. В IntelliJ IDEA создать новый проект из git (https://github.com/ververica/flink-training-exercises)
2. Установить версию java 1.8 (почему-то с последними версиями выпадали ошибки)
3. Удалить все .scala файлы, на которые будет ругаться среда
4. Перед выполнением заданий указать путь к данным в переменных ```pathToRideData``` и ```pathToFareData``` в файле ```./flink-training-exercises/src/main/java/com/ververica/flinktraining/exercises/datastream_java/utils/ExerciseBase.java```

## Тесты
https://github.com/Anteii/HPC-Labs/blob/main/lab4/resources/cpu_gpu_times_small_n_subs.png
<p align="center">
  <b>Expiring State Test</b><br>
  <img src="ExpiringStateTest.png"/>
</p>

<p align="center">
  <b>Hourly Tips Test</b><br>
  <img src="HourlyTipsTest.png"/>
</p>

<p align="center">
  <b>Ride Cleansing Test</b><br>
  <img src="RideCleansingTest.png"/>
</p>

<p align="center">
  <b>Rides And Fares Test</b><br>
  <img src="RidesAndFaresTest.png"/>
</p>

## Использованные источники

* https://git.ai.ssau.ru/tk/big_data/src/branch/master/L3%20-%20Stream%20processing%20with%20Apache%20Flink
*  https://training.ververica.com (различные обучающие материалы)
* https://stackoverflow.com/questions/68149415/apache-flink-job-crashes-with-exception-java-lang-illegalaccesserror-class-org