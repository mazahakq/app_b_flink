package ru.mazahakq.appb.state;

import ru.mazahakq.appb.dto.*;
import ru.mazahakq.appb.operation.Log;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

// Класс SaveToState должен быть изменен, чтобы возвращать тип String
public class SaveToState extends RichFlatMapFunction<Message, String> {
    private transient MapState<Long, Message> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<Long, Message> descriptor =
                new MapStateDescriptor<>("message-state", Long.class, Message.class);
        this.mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void flatMap(Message value, Collector<String> out) throws Exception {
        Long number = value.getNumber();
        String guid = value.getGuid();
        mapState.put(number, value);
        Log.logger.info("Processing kafka for message number '{}' guid '{}'", number, guid);
        // Возвращаем информационное сообщение
        out.collect("Processed message with number: " + number);
    }
}