package com.example.app_b_flink.processor;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import com.example.app_b_flink.model.*;
import com.example.app_b_flink.state.MessageState;

public class NumberProcessor extends CoProcessFunction<KafkaMessage, RabbitMQRequest, ResultResponse> {

    private MapState<String, KafkaMessage> mapState;

    @Override
    public void open(Configuration config) throws Exception {
        MapStateDescriptor<String, KafkaMessage> desc = new MapStateDescriptor<>(
            "messageState",
            Types.STRING,
            Types.POJO(KafkaMessage.class));
        mapState = getRuntimeContext().getMapState(desc);
    }

    @Override
    public void processElement1(KafkaMessage msg, Context ctx, Collector<ResultResponse> out) throws Exception {
        // Сохраняем сообщение из Kafka в state
        mapState.put(msg.getNumber(), msg);
    }

    @Override
    public void processElement2(RabbitMQRequest request, Context ctx, Collector<ResultResponse> out) throws Exception {
        KafkaMessage storedMsg = mapState.get(request.getNum1() + "");
        if(storedMsg != null){
            int result = request.getNum1() + request.getNum2();
            ResultResponse response = new ResultResponse(request.getNum1(),
                                                         result,
                                                         request.getCorrId(),
                                                         storedMsg.getGuid());
            out.collect(response);
        }
    }
}