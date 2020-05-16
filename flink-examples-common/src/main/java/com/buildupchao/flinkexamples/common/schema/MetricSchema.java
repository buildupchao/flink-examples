package com.buildupchao.flinkexamples.common.schema;

import com.buildupchao.flinkexamples.common.model.MetricEvent;
import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * @author buildupchao
 * @date 2020/5/16 15:54
 * @since JDK1.8
 **/
public class MetricSchema implements DeserializationSchema<MetricEvent>, SerializationSchema<MetricEvent> {

    private static final Gson gson = new Gson();

    @Override
    public MetricEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), MetricEvent.class);
    }

    @Override
    public boolean isEndOfStream(MetricEvent metricEvent) {
        return false;
    }

    @Override
    public byte[] serialize(MetricEvent metricEvent) {
        return gson.toJson(metricEvent).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<MetricEvent> getProducedType() {
        return TypeInformation.of(MetricEvent.class);
    }
}
