package ru.mazahakq.appb;

import ru.mazahakq.appb.dto.*;
import ru.mazahakq.appb.state.*;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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
        env.enableCheckpointing(600000, CheckpointingMode.EXACTLY_ONCE); //Checkpoint раз в 10 минут

        //KAFKA Источник
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092") //Адрес брокера
                .setTopics("messages_topic") //Топик
                .setGroupId("app_b_flink") //Consumer Group
                .setStartingOffsets(OffsetsInitializer.earliest()) //Политика
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        //RabbitMQ Источник
        RMQConnectionConfig rabbitConnectionConfig = new RMQConnectionConfig.Builder()
                .setHost("rabbitmq") // Хост RabbitMQ
                .setPort(5672) // Порт RabbitMQ
                .setUserName("guest") // Пользователь
                .setPassword("guest") // Пароль
                .setVirtualHost("/") // Виртуальная среда
                .build();

        //Получение сообщений из Kafka и Сохраняем в Flink State
        DataStream<MessageInput> resultStreamkafka = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),
                "Kafka Source")
                .map(message -> {
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.readValue(message, MessageInput.class);
                })
                .keyBy(MessageInput::getNumber) //Ключ по Number полю
                .flatMap(new SaveToState());
    
        //Получение сообщений из RabbitMQ
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
            .keyBy(RequestMessage::getNum1); //Ключ по Num1 полю

        //Объединение потоков для возможности доступа в Flink State из Потока RabbitMQ
        DataStream<Tuple2<MessageInput, RequestMessage>> joinedStreamsWithWindow = resultStreamkafka.join(rabbitGroupedStream)
            .where(MessageInput::getNumber)
            .equalTo(RequestMessage::getNum1)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
            .apply((msg, reqMsg) -> Tuple2.of(msg, reqMsg), TypeInformation.of(new TypeHint<Tuple2<MessageInput, RequestMessage>>() { }));

        //Поиск данных в Flink State и генерация ответа для RabbirMQ
        DataStream<String> outputStreams = 
            joinedStreamsWithWindow.keyBy(tup -> tup.f1.getNum1())
            .flatMap(new SearchInState()).name("Processing Stage");

        //Отправка ответа в RabbitMQ
        outputStreams.addSink(new RMQSink<>(
                rabbitConnectionConfig, // Конфигурация подключения
                "result_queue", // Название выходной очереди
                new SimpleStringSchema() // Сериализатор
        )).name("Sink Stage");

        // Выполнение задания
        env.execute("app_b_flink");
    }
}