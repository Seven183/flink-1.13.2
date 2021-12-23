package order.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.rocketmq.flink.legacy.common.serialization.KeyValueDeserializationSchema;

import java.nio.charset.StandardCharsets;

public class SimpleStringDeserializationSchema implements KeyValueDeserializationSchema<String> {

    private static final long serialVersionUID = 3629052169765354165L;

    @Override
    public String deserializeKeyAndValue(byte[] key, byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
