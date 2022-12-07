# Лабораторная работа 4

## Содержание

* [Выбор инструментов](#выбор-инструментов)
* [Установка, развертывание, разработка (zoo)](#решение-zoo)
* [Установка, развертывание, разработка (философы и двуфазный коммит)](#решение-других-задач)
* [Результаты](#результаты)
* [Ссылки на использованные материалы](#ссылки-на-использованные-материалы)

## Выбор инструментов

Для задания zoo (которое почти полностью было разобрано в задании) были использованы следующие технологии:
* open_jdk 1.8.0_42
* IntelliJ IDEA 2021.3.3
* Scala 2.13.10
* sbt 1.8.0

При решении задачи об обедающих философах и реализации двух-фазного коммита использовался язык программирования python и библиотека kazoo.
## Решение zoo

1. В IntelliJ IDEA установть плагин Scala
2. В IntelliJ IDEA создать проект scala, сборщик sbt
3. В IntelliJ IDEA открыть настройки sbt и установить параметр Maximum Heap size, MB (для слишком больших ималеньких крашит sbt, по умолчанию - пытается взять больше чем есть. Для меня сработало значение 1500)

        File -> Settings -> Build, Excecution, Deployment -> Build Tools -> sbt (поле Maximum Heap size, MB)

4. После завершения написания кода создать две конфигурации запуска приложения (консольные аргументы установить имя_животного localhost:2181 2)

5. Запустить сервер Zookeeper

        zkServer.cmd 

6. Запустить приложение с помощью конфигураций

## Решение других задач

1. Установить библиотеку kazoo (версия 2.9.0 содержит критические баги)

        pip install kazoo==2.8.0

2. Написать код приложения
3. Запустить сервер Zookeeper

        zkServer.cmd 

4. Запустить приложение

## Результаты

### zoo

<p align="center">
  <b>Приложение Monkey</b><br>
  <img src="https://github.com/Anteii/big-data-labs/blob/main/screenshots/tiger_output.png"/>
</p>

<p align="center">
  <b>Приложение Tiger</b><br>
  <img src="https://github.com/Anteii/big-data-labs/blob/main/screenshots/monkey_output.png"/>
</p>

### Философы 

<p align="center">
  <b>Вывод в консоль</b><br>
  <img src="https://github.com/Anteii/big-data-labs/blob/main/screenshots/philosopher_output.png"/>
</p>

### Двуфазный коммит

<p align="center">
  <b>Вывод в консоль</b><br>
  <img src="https://github.com/Anteii/big-data-labs/blob/main/screenshots/2pc_output.png"/>
</p>

7. В интерфейсе jupyter создаем новый ноутбук

## Пояснения к заданию находятся в jupter ноутбуке
## Ссылки на использованные материалы
* https://git.ai.ssau.ru/tk/big_data/src/branch/master/L4%20-%20ZooKeeper
* https://github.com/python-zk/kazoo/issues/679
* https://stackoverflow.com/questions/48627793/oserror-winerror-87the-parameter-is-incorrect
* https://stackoverflow.com/questions/57020980/getting-oserror-winerror-87-and-type-error-cant-pickle-thread-lock-objects
* https://kazoo.readthedocs.io/en/latest/api.html
* https://ru.wikipedia.org/wiki/%D0%97%D0%B0%D0%B4%D0%B0%D1%87%D0%B0_%D0%BE%D0%B1_%D0%BE%D0%B1%D0%B5%D0%B4%D0%B0%D1%8E%D1%89%D0%B8%D1%85_%D1%84%D0%B8%D0%BB%D0%BE%D1%81%D0%BE%D1%84%D0%B0%D1%85
* https://zookeeper.apache.org/doc/r3.4.2/recipes.html#sc_recipes_twoPhasedCommit
* https://stackoverflow.com/questions/24635777/how-to-implement-2pc-in-zookeeper-cluster
