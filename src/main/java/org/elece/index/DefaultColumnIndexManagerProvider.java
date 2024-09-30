package org.elece.index;

import org.elece.config.DbConfig;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.schema.type.TableWithNoPrimaryKeyError;
import org.elece.exception.storage.StorageException;
import org.elece.memory.Pointer;
import org.elece.memory.data.BinaryObjectFactory;
import org.elece.memory.data.PointerBinaryObject;
import org.elece.memory.tree.node.DefaultNodeFactory;
import org.elece.serializer.Serializer;
import org.elece.serializer.SerializerRegistry;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Column;
import org.elece.sql.db.schema.model.Table;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.storage.index.IndexStorageManagerFactory;
import org.elece.storage.index.session.factory.AtomicIOSessionFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DefaultColumnIndexManagerProvider extends ColumnIndexManagerProvider {
    private final Map<String, IndexManager<?, ?>> indexManagers = new HashMap<>();

    public DefaultColumnIndexManagerProvider(DbConfig dbConfig, IndexStorageManagerFactory indexStorageManagerFactory) {
        super(dbConfig, indexStorageManagerFactory);
    }

    @Override
    public IndexManager<?, ?> getIndexManager(Table table, Column column) throws SchemaException, StorageException {
        IndexId indexId = new IndexId(table.getId(), column.getId());
        String indexIdString = indexId.asString();
        if (indexManagers.containsKey(indexIdString)) {
            return indexManagers.get(indexIdString);
        } else {
            IndexManager<?, ?> indexManager = buildIndexManager(table, column);
            indexManagers.put(indexIdString, indexManager);
            return indexManager;
        }
    }

    @Override
    public void clearIndexManager(Table table, Column column) {
        IndexId indexId = new IndexId(table.getId(), column.getId());
        indexManagers.remove(indexId.asString());
    }

    private <K extends Comparable<K>> IndexManager<K, ?> buildIndexManager(Table table, Column column) throws StorageException, SchemaException {
        IndexId indexId = new IndexId(table.getId(), column.getId());

        Serializer<K> serializer = SerializerRegistry.getInstance().getSerializer(column.getSqlType().getType());

        if (column.getConstraints().contains(SqlConstraint.PrimaryKey)) {
            BinaryObjectFactory<K> kBinaryObjectFactory = serializer.getBinaryObjectFactory(column);
            BinaryObjectFactory<Pointer> vBinaryObjectFactory = new PointerBinaryObject.Factory();

            return new TreeIndexManager<>(
                    indexId.asInt(),
                    indexStorageManagerFactory.create(indexId),
                    AtomicIOSessionFactory.getInstance(dbConfig),
                    dbConfig,
                    kBinaryObjectFactory,
                    vBinaryObjectFactory,
                    new DefaultNodeFactory<>(kBinaryObjectFactory, vBinaryObjectFactory)
            );
        } else {
            Optional<Column> primaryColumn = SchemaSearcher.findPrimaryColumn(table);
            if (primaryColumn.isEmpty()) {
                throw new SchemaException(new TableWithNoPrimaryKeyError(table.getName()));
            }

            Serializer<?> primarySerializer = SerializerRegistry.getInstance().getSerializer(primaryColumn.get().getSqlType().getType());

            BinaryObjectFactory<K> kBinaryObjectFactory = serializer.getBinaryObjectFactory(column);
            BinaryObjectFactory<?> vBinaryObjectFactory = primarySerializer.getBinaryObjectFactory(primaryColumn.get());
            return new TreeIndexManager<>(
                    indexId.asInt(),
                    indexStorageManagerFactory.create(indexId),
                    AtomicIOSessionFactory.getInstance(dbConfig),
                    dbConfig,
                    kBinaryObjectFactory,
                    vBinaryObjectFactory,
                    new DefaultNodeFactory<>(kBinaryObjectFactory, vBinaryObjectFactory)
            );
        }
    }
}
