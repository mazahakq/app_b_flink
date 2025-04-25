package ru.mazahakq.appb;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;

public class Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("rabbitmq")       // Хост RabbitMQ
                .setPort(5672)            // Порт RabbitMQ
                .setUserName("guest")     
                .setPassword("guest")     // Пароль
                .setVirtualHost("/")      // Виртуальное пространство
                .build();

        DeserializationSchema<byte[]> customDeserializer = new CustomByteArrayDeserializer();

        // Прием данных в виде байтов
        DataStream<byte[]> sourceStream = env.addSource(new RMQSource<>(
                connectionConfig,          // Конфигурация подключения
                "rpc_queue",               // Название очереди
                false,                     // Ручное подтверждение
                customDeserializer         // Кастомный десериализатор
        ), TypeInformation.of(byte[].class)); // Тип данных потока

        // Далее идёт обработка потока...
        sourceStream.process(new RpcHandler());

        env.execute("app-b-flink-rpc");
    }
}
