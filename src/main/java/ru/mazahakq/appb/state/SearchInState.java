package ru.mazahakq.appb.state;

import ru.mazahakq.appb.dto.*;
import ru.mazahakq.appb.operation.Log;
import ru.mazahakq.appb.operation.ProcessingAgg;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;

/**
 * FlatMap Function для обработки и поиска в Keyed State.
 */
public class SearchInState extends RichFlatMapFunction<RequestMessage, String> {
    private transient MapState<Long, Message> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<Long, Message> descriptor =
                new MapStateDescriptor<>("message-state", Long.class, Message.class);
        this.mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void flatMap(RequestMessage value, Collector<String> out) throws Exception {
        Instant startTime = Instant.now(); // Начало обработки
        ObjectMapper mapper = new ObjectMapper();
        ResponseMessage response = ProcessingAgg.operationMessage(value);
        String corrId = response.getCorr_id();
        Long result = response.getResult();
        Log.logger.info("Processing time for message id '{}' search result '{}'", corrId, result);
        Message storedMessage = mapState.get(result);
        if(storedMessage != null) {
            response.setGuid(storedMessage.getGuid());
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