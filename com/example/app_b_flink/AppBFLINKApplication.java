package com.example.app_b_flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.rabbitmq.sink.RabbitMQSink;
import org.apache.flink.connector.rabbitmq.source.RabbitMQSource;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import com.example.app_b_flink.connector.*;
import com.example.app_b_flink.model.*;
import com.example.app_b_flink.processor.NumberProcessor;
import com.example.app_b_flink.state.MessageState;

public class AppBFLINKApplication {

    public static void main(String[] args) throws Exception {
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("file:///tmp/flink-checkpoints"));

        // Подключение к Kafka и чтение сообщений
        KafkaSource<KafkaMessage> kafkaSource = KafkaSource.<KafkaMessage>builder()
                                                        .setBootstrapServers("localhost:9092")
                                                        .setTopics("messages_topic")
                                                        .setGroupId("my-group-id")
                                                        .setStartingOffsets(KafkaOffsetsInitializer.latest())
                                                        .setDeserializer(new SimpleStringSchema())
                                                        .build();

        DataStream<KafkaMessage> kafkaMessages = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        // Чтение сообщений из RabbitMQ
        RabbitMQSource<RabbitMQRequest> rabbitMQSource = RabbitMQSource.<RabbitMQRequest>builder()
                                                                      .setAddresses("localhost")
                                                                      .setQueueName("numbers_queue")
                                                                      .setDeserializer(new SimpleStringSchema())
                                                                      .build();

        DataStream<RabbitMQRequest> rabbitMQRequests = env.fromSource(rabbitMQSource, WatermarkStrategy.noWatermarks(), "rabbitmq-source");

        // Соединяем два потока и выполняем обработку
        DataStream<ResultResponse> processedResults = kafkaMessages.connect(rabbitMQRequests)
                                                                 .process(new NumberProcessor());

        // Создание sink'а для записи в RabbitMQ
        RabbitMQSink<ResultResponse> rabbitMQSink = RabbitMQSink.builder()
                                                               .setHost("localhost")
                                                               .setExchange("result_exchange")
                                                               .setRoutingKey("result_queue")
                                                               .setSerializer(new SimpleStringSchema())
                                                               .build();

        processedResults.addSink(rabbitMQSink);

        env.execute("App B FLINK Application");
    }
}