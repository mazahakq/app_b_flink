package ru.mazahakq.appb.state;

import ru.mazahakq.appb.dto.*;
import ru.mazahakq.appb.operation.Log;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.databind.ObjectMapper;

// Получение данных из flink state для генерации ответа в RabbitMQ
public class OperationState extends CoProcessFunction<MessageInput, RequestMessage, String> {

    private MapState<Long, MessageInput> mapState;

    @Override
    public void open(Configuration config) throws Exception {
        MapStateDescriptor<Long, MessageInput> desc = new MapStateDescriptor<>(
                "messageState",
                Types.LONG,
                Types.POJO(MessageInput.class));
        mapState = getRuntimeContext().getMapState(desc);
    }

    @Override
    public void processElement1(MessageInput msg, Context ctx, Collector<String> out) throws Exception {
        // Сохраняем сообщение из Kafka в state
        mapState.put(msg.getNumber(), msg);
        Log.logger.info("Processing kafka for message number '{}' guid '{}'", msg.getNumber(), msg.getGuid());
    }

    @Override
    public void processElement2(RequestMessage request, Context ctx, Collector<String> out) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        MessageInput storedMsg = mapState.get(request.getNum1());
        Long result = request.getNum1() + request.getNum2();
        Log.logger.info("Processing for message id '{}' search number '{}'", request.getCorr_id(), request.getNum1());
        if (storedMsg != null) {
            ResponseMessage response = new ResponseMessage(request.getNum1(),
                    result,
                    request.getCorr_id(),
                    storedMsg.getGuid());
            Log.logger.info("Response for message id '{}' guid '{}'", response.getCorr_id(), response.getGuid());
            out.collect(mapper.writeValueAsString(response));
        } else {
            ResponseMessage response = new ResponseMessage(request.getNum1(),
                    result,
                    request.getCorr_id(),
                    "");
            out.collect(mapper.writeValueAsString(response));
        }
    }
}