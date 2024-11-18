package org.elece.query.plan.step.filter;

import org.elece.db.DbObject;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.DeserializationException;
import org.elece.exception.ParserException;
import org.elece.query.path.ComplexPathNode;
import org.elece.serializer.Serializer;
import org.elece.serializer.SerializerRegistry;
import org.elece.sql.parser.expression.internal.*;
import org.elece.utils.SerializationUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ComplexFieldFilterStep extends FilterStep {
    private final Table table;
    private final ComplexPathNode pathNode;
    private final SerializerRegistry serializerRegistry;

    public ComplexFieldFilterStep(Table table, ComplexPathNode pathNode, SerializerRegistry serializerRegistry,
                                  Long scanId) {
        super(scanId);
        this.table = table;
        this.pathNode = pathNode;
        this.serializerRegistry = serializerRegistry;
    }

    @Override
    public boolean next(DbObject dbObject) throws ParserException, DeserializationException {
        Set<String> identifiers = pathNode.getIdentifiers();

        Map<String, SqlValue<?>> values = new HashMap<>();
        for (String identifier : identifiers) {
            Optional<Column> column = SchemaSearcher.findColumn(table, identifier);
            if (column.isEmpty()) {
                return false;
            }
            byte[] valueOfField = SerializationUtils.getValueOfField(table, column.get(), dbObject);
            SqlType.Type type = column.get().getSqlType().getType();
            SqlValue<?> value = switch (type) {
                case INT -> {
                    Serializer<Integer> serializer = serializerRegistry.getSerializer(type);
                    yield new SqlNumberValue(serializer.deserialize(valueOfField, column.get()));
                }
                case BOOL -> {
                    Serializer<Boolean> serializer = serializerRegistry.getSerializer(type);
                    yield new SqlBoolValue(serializer.deserialize(valueOfField, column.get()));
                }
                case VARCHAR -> {
                    Serializer<String> serializer = serializerRegistry.getSerializer(type);
                    yield new SqlStringValue(serializer.deserialize(valueOfField, column.get()).trim());
                }
            };
            values.put(identifier, value);
        }
        return pathNode.resolve(values);
    }
}
