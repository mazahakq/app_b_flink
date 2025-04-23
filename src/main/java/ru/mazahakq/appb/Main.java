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
        env.enableCheckpointing(600000, CheckpointingMode.EXACTLY_ONCE);
        // env.disableOperatorChaining(); // Запрещаем объединение узлов
        // env.setParallelism(1); // Устанавливаем глобальную параллельность

        // Настройка конфигурации подключения к RabbitMQ
        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("rabbitmq") // Хост RabbitMQ
                .setPort(5672) // Порт RabbitMQ
                .setUserName("guest") // Пользователь
                .setPassword("guest") // Пароль
                .setVirtualHost("/") // Виртуальная среда
                .build();

        //KAFKA
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("messages_topic")
                .setGroupId("app_b_flink")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> kafkaInputStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),
                "Kafka Source");
        DataStream<Message> kafkaMappedStream = kafkaInputStream.map(message -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(message, Message.class);
        });
        DataStream<Message> kafkaGroupedStream = kafkaMappedStream.keyBy(Message::getNumber);
        kafkaGroupedStream.flatMap(new SaveToState());

        // Stage 1: Create separate SOURCE stage
        DataStream<String> inputStream = env.addSource(new RMQSource<>(
                connectionConfig, // Конфигурация подключения
                "numbers_queue", // Название очереди
                true, // Автоматическое подтверждение
                new SimpleStringSchema() // Десериализатор (например, для строк)
        )).name("Source Stage");

        // Stage 2: Processing stage
        DataStream<RequestMessage> mappedStream = inputStream.map(message -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(message, RequestMessage.class);
        });
        DataStream<RequestMessage> groupedStream = mappedStream.keyBy(value -> value.hashCode());
        DataStream<String> resultStream = groupedStream.flatMap(new SearchInState()).name("Processing Stage");

        // Stage 3: Separate SINK stage
        resultStream.addSink(new RMQSink<>(
                connectionConfig, // Конфигурация подключения
                "result_queue", // Название выходной очереди
                new SimpleStringSchema() // Сериализатор
        )).name("Sink Stage");

        // Выполнение задания
        env.execute("app_b_flink");
    }
}