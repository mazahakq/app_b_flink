package ru.mazahakq.appb;

import ru.mazahakq.appb.dto.*;
import ru.mazahakq.appb.state.*;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000, CheckpointingMode.AT_LEAST_ONCE); // Checkpoint раз в 10 минут

        // Kafka Конфигурация
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092") // Адрес брокера
                .setTopics("messages_topic") // Топик
                .setGroupId("app_b_flink") // Consumer Group
                .setStartingOffsets(OffsetsInitializer.earliest()) // Политика
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // RabbitMQ Конфигурация
        RMQConnectionConfig rabbitConnectionConfig = new RMQConnectionConfig.Builder()
                .setHost("rabbitmq") // Хост RabbitMQ
                .setPort(5672) // Порт RabbitMQ
                .setUserName("guest") // Пользователь
                .setPassword("guest") // Пароль
                .setVirtualHost("/") // Виртуальная среда
                .build();

        // Получение сообщений из Kafka и Сохраняем в Flink State
        DataStream<MessageInput> resultStreamkafka = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),
                "Kafka Source")
                .map(message -> {
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.readValue(message, MessageInput.class);
                })
                .keyBy(MessageInput::getNumber); // Ключ по Number полю

        // Получение сообщений из RabbitMQ
        DataStream<RequestMessage> rabbitGroupedStream = env.addSource(new RMQSource<>(
                rabbitConnectionConfig, // Конфигурация подключения
                "numbers_queue", // Название очереди
                true, // Автоматическое подтверждение
                new SimpleStringSchema() // Десериализатор (например, для строк)
        ))
                .map(message -> {
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.readValue(message, RequestMessage.class);
                })
                .name("RabbitMQ Source")
                .keyBy(RequestMessage::getNum1); // Ключ по Num1 полю

        // Объединение потоков для возможности доступа в Flink State из Потока RabbitMQ
        // и Поиск данных в Flink State и генерация ответа для RabbirMQ
        DataStream<String> outputStreams = resultStreamkafka.connect(rabbitGroupedStream)
                .process(new OperationState()).name("Aggregation Stage");

        // Отправка ответа в RabbitMQ
        outputStreams.addSink(new RMQSink<>(
                rabbitConnectionConfig, // Конфигурация подключения
                "result_queue", // Название выходной очереди
                new SimpleStringSchema() // Сериализатор
        )).name("RabbitMQ Sink Stage");

        // Выполнение задания
        env.execute("app_b_flink");
    }
}