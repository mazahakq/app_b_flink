Приложение app-b-flink-state реализовано для flink 1.16.1 java (maven) выполняет следующие действия:  
```
- Получает поток из очереди numbers_queue RabbitMQ, сообщения формата {"num1": "Число", "num2": "Число", "corr_id": "Уникальный идентификатор запроса, строка", "timestamp": "Метка времени, строка" }
- Возвращает ответ в очередь result_queue RabbitMQ, суммирует числа num1 и num2 для ответа, сообщения формата {"number": "Число", "result": "Сумма чисел num1 и num2", "corr_id": "Уникальный идентификатор запроса, строка", "guid": "Уникальный идентификатор guid, строка" }
```

Приложение app-b-flink-state реализовано для flink 1.16.1 java (maven) выполняет следующие действия:  
```
- Получает из топика messages_topic kafka сообщения, формата {"number": "Число", "guid": "Уникальный идентификатор guid, строка" }
- Записывает данные сообщения из kafka в flink state, ключевое поле number
- Параллельно, получает поток из очереди numbers_queue RabbitMQ, сообщения формата {"num1": "Число", "num2": "Число", "corr_id": "Уникальный идентификатор запроса, строка", "timestamp": "Метка времени, строка" }
- Выполняет поиск по данным записанным потоком kafka в flink state по атрибуту num1 из сообщения kafka, извлекает из state значение guid для данного номера, а так же суммирует числа num1 и num2 для ответа
- Возвращает ответ в очередь result_queue RabbitMQ, сообщения формата {"number": "Число", "result": "Сумма чисел num1 и num2", "corr_id": "Уникальный идентификатор запроса, строка", "guid": "Уникальный идентификатор guid, строка" }
```

Добавить библиотека коннектора rabbitmq в локальный репозиторий.  
В данной библиотеке внесены изменения в части автоматического поддтверждения получения сообщений из RabbitMQ "autoAck = true;"  
Данное изменение необходимо для работы в режиме с включенными checkpoint-ами, чтобы не накапливать Unacked сообщения в очереди RabbitMQ до выполнения checkpoint-а.  
Причина такого поведения, логика flink позволяющая перечитывать сообщения повторно.  
```
mvn install:install-file \
    -Dfile=./libs/flink-connector-rabbitmq-1.16.1-autoAck.jar \
    -DpomFile=./libs/flink-connector-rabbitmq-1.16.1.pom
    -DgroupId=org.apache.flink \
    -DartifactId=flink-connector-rabbitmq \
    -Dversion=1.16.1-autoAck \
    -Dpackaging=jar \
```

Для сборки приложения выполните:  
```
mvn clean package
```

Пример файлов собранных приложений для flink:  
```
app-b-flink/target/app-b-flink-1.0.1.jar
app-b-flink-state/target/app-b-flink-state-2.0.0.jar
```
