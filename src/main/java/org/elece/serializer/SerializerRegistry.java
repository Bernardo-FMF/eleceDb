package org.elece.serializer;

import org.elece.sql.parser.expression.internal.SqlType;

import java.util.HashMap;
import java.util.Map;

public class SerializerRegistry {
    private static final SerializerRegistry instance = new SerializerRegistry();

    private final Map<SqlType.Type, Serializer<?>> serializers;

    public SerializerRegistry() {
        this.serializers = new HashMap<>();
        this.serializers.put(SqlType.Type.INT, new IntegerSerializer());
        this.serializers.put(SqlType.Type.BOOL, new BooleanSerializer());
        this.serializers.put(SqlType.Type.VARCHAR, new StringSerializer());
    }

    public static SerializerRegistry getInstance() {
        return instance;
    }

    public <K extends Comparable<K>> Serializer<K> getSerializer(SqlType.Type type) {
        return (Serializer<K>) serializers.get(type);
    }
}
