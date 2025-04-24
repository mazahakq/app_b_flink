package ru.mazahakq.appb.state;

import ru.mazahakq.appb.dto.*;
import ru.mazahakq.appb.operation.Log;
import ru.mazahakq.appb.operation.ProcessingAgg;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;

// Извлечение данных из Flink state для получения ответа в RabbitMQ
public class SearchInState extends RichFlatMapFunction<Tuple2<MessageInput, RequestMessage>, String> {
    private transient MapState<Long, MessageInput> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<Long, MessageInput> descriptor =
                new MapStateDescriptor<>("message-state", Long.class, MessageInput.class);
        this.mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<MessageInput, RequestMessage> value, Collector<String> out) throws Exception {
        Instant startTime = Instant.now(); // Начало обработки
        ObjectMapper mapper = new ObjectMapper();
        ResponseMessage response = ProcessingAgg.operationMessage(value.f1);
        String corrId = response.getCorr_id();
        Long number = response.getNumber();
        Log.logger.info("Processing time for message id '{}' search number '{}'", corrId, number);
        MessageInput storedMessage = mapState.get(number);
        if(storedMessage != null) {
            String guid = storedMessage.getGuid();
            response.setGuid(guid);
            out.collect(mapper.writeValueAsString(response));
            Log.logger.info("Processing time for message id '{}' exists guid state", corrId);
        } else {
            out.collect(mapper.writeValueAsString(response));
            Log.logger.info("Processing time for message id '{}' not guid state", corrId);
        }
        Instant finishTime = Instant.now(); // Окончание обработки
        long durationMs = Duration.between(startTime, finishTime).toMillis();
        Log.logger.info("Processing time for message id '{}': {} ms", corrId, durationMs);
    }
}