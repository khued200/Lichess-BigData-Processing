@echo on
set KAFKA_HOME=C:\BigData\kafka

echo Starting Zookeeper...
start %KAFKA_HOME%\bin\windows\zookeeper-server-start.bat %KAFKA_HOME%\config\zookeeper.properties
timeout 30

echo Starting Kafka...
start %KAFKA_HOME%\bin\windows\kafka-server-start.bat %KAFKA_HOME%\config\server.properties

echo Kafka and Zookeeper have been started.

