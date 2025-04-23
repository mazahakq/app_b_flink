package ru.mazahakq.appb;
import ru.mazahakq.appb.state.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

public class Main2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE); // Настройка чекпоинтинга каждые 5 секунд

		KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("messages_topic")
            .setGroupId("app_b_flink")
		    .setStartingOffsets(OffsetsInitializer.earliest())
		    .setValueOnlyDeserializer(new SimpleStringSchema())
		    .build();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source").flatMap(new SaveToState());

        // Выполнение задания
        env.execute("app_b");
    }
}