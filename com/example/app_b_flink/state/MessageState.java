package com.example.app_b_flink.state;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.example.app_b_flink.model.KafkaMessage;

public class MessageState implements ListCheckpointed<Tuple2<String, KafkaMessage>> {

    private transient MapState<String, KafkaMessage> mapState;

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        MapStateDescriptor<String, KafkaMessage> desc = new MapStateDescriptor<>(
            "messageState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<KafkaMessage>() {}));
        mapState = context.getOperatorStateStore().getUnionListState(desc);
    }

    @Override
    public Iterable<Tuple2<String, KafkaMessage>> snapshotState(long checkpointId, long timestamp) throws Exception {
        return mapState.entries();
    }

    public void put(String key, KafkaMessage value) {
        mapState.put(key, value);
    }

    public KafkaMessage get(String key) {
        return mapState.get(key);
    }
}