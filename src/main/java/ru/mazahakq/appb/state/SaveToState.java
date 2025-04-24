package ru.mazahakq.appb.state;

import ru.mazahakq.appb.dto.*;
import ru.mazahakq.appb.operation.Log;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

// Класс SaveToState должен быть изменен, чтобы возвращать тип String
public class SaveToState extends RichFlatMapFunction<MessageInput, MessageInput> {
    private transient MapState<Long, MessageInput> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<Long, MessageInput> descriptor =
                new MapStateDescriptor<>("message-state", Long.class, MessageInput.class);
        this.mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void flatMap(MessageInput value, Collector<MessageInput> out) throws Exception {
        Long number = value.getNumber();
        String guid = value.getGuid();
        mapState.put(number, value);
        Log.logger.info("Processing kafka for message number '{}' guid '{}'", number, guid);
        // Возвращаем информационное сообщение
        out.collect(value);
    }
}