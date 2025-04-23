package ru.mazahakq.appb.state;

import ru.mazahakq.appb.dto.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

// Класс SaveToState должен быть изменен, чтобы возвращать тип String
public class SaveToState extends RichFlatMapFunction<Message, String> {
    private transient ValueState<MessageState> messageState;

    @Override
    public void open(Configuration parameters) throws Exception {
        messageState = getRuntimeContext().getState(new ValueStateDescriptor<>("message_state", MessageState.class));
    }

    @Override
    public void flatMap(Message value, Collector<String> out) throws Exception {
        int number = value.getNumber();
        MessageState currentState = messageState.value();

        // Если состояние для этого ключа еще не создано, создаем новое
        if (currentState == null) {
            currentState = new MessageState(value.getGuid(), value.getNumber());
        } else {
            currentState.setGuid(value.getGuid());
        }

        // Обновляем состояние
        messageState.update(currentState);

        // Возвращаем информационное сообщение
        out.collect("Processed message with number: " + number);
    }
}