package ru.mazahakq.appb.state;

import ru.mazahakq.appb.dto.*;
import ru.mazahakq.appb.operation.Log;
import ru.mazahakq.appb.operation.ProcessingAgg;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
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
    private transient ValueState<MessageState> messageState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // Настройка состояния
        messageState = getRuntimeContext().getState(new ValueStateDescriptor<>("message_state", MessageState.class));
    }

    @Override
    public void flatMap(RequestMessage value, Collector<String> out) throws Exception {
        Instant startTime = Instant.now(); // Начало обработки
        ObjectMapper mapper = new ObjectMapper();
        ResponseMessage response = ProcessingAgg.operationMessage(value);
        String corrId = response.getCorr_id();
        int result = response.getResult();
        MessageState currentState = messageState.value();
        if (currentState != null && currentState.getNumber() == result) {
            response.setGuid(currentState.getGuid());
            out.collect(mapper.writeValueAsString(response));
        } else {
            out.collect(mapper.writeValueAsString(response));
            Log.logger.info("Processing time for message id '{}' not guid state", corrId);
        }
        Instant finishTime = Instant.now(); // Окончание обработки
        long durationMs = Duration.between(startTime, finishTime).toMillis();
        Log.logger.info("Processing time for message id '{}': {} ms", corrId, durationMs);
    }
}