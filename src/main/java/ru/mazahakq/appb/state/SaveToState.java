package ru.mazahakq.appb.state;

import ru.mazahakq.appb.dto.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * FlatMap Function для обработки и сохранения данных в Keyed State.
 */
public class SaveToState extends RichFlatMapFunction<String, String> {
    private transient ValueState<MessageState> messageState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // Настройка состояния
        messageState = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("message_state", MessageState.class));
    }

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Message incomingMessage = mapper.readValue(value, Message.class);

        int number = incomingMessage.getNumber();
        MessageState currentState = messageState.value();

        // Если состояние для этого ключа еще не создано, создаем новое
        if (currentState == null) {
            currentState = new MessageState(incomingMessage.getGuid(), incomingMessage.getNumber());
        } else {
            currentState.setGuid(incomingMessage.getGuid());
        }

        // Обновляем состояние
        messageState.update(currentState);

        // Выводим информацию о сохранении
        out.collect("Saved message with number: " + number);
    }
}