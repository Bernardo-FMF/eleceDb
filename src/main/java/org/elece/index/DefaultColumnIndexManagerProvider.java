package org.elece.index;

import org.elece.config.DbConfig;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.DbError;
import org.elece.exception.SchemaException;
import org.elece.exception.StorageException;
import org.elece.memory.data.BinaryObjectFactory;
import org.elece.memory.tree.node.DefaultNodeFactory;
import org.elece.serializer.Serializer;
import org.elece.serializer.SerializerRegistry;
import org.elece.storage.index.IndexStorageManagerFactory;
import org.elece.storage.index.session.factory.AtomicIOSessionFactory;

import java.util.HashMap;
import java.util.Map;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

public class DefaultColumnIndexManagerProvider extends ColumnIndexManagerProvider {
    private final Map<String, IndexManager<?, ?>> indexManagers = new HashMap<>();

    public DefaultColumnIndexManagerProvider(DbConfig dbConfig, IndexStorageManagerFactory indexStorageManagerFactory) {
        super(dbConfig, indexStorageManagerFactory);
    }

    @Override
    public IndexManager<?, ?> getClusterIndexManager(Table table) throws SchemaException, StorageException {
        Column clusterColumn = SchemaSearcher.findClusterColumn(table);
        return getIndexManager(table, clusterColumn);
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

    private <K extends Comparable<K>, V extends Comparable<V>> IndexManager<K, ?> buildIndexManager(Table table,
                                                                                                    Column column) throws
                                                                                                                   StorageException,
                                                                                                                   SchemaException {
        IndexId indexId = new IndexId(table.getId(), column.getId());

        Serializer<K> serializer = SerializerRegistry.getInstance().getSerializer(column.getSqlType().getType());

        if (CLUSTER_ID.equals(column.getName())) {
            BinaryObjectFactory<K> kBinaryObjectFactory = serializer.getBinaryObjectFactory(column);

            return new ClusterTreeIndexManager<>(
                    indexId.asInt(),
                    indexStorageManagerFactory.create(indexId),
                    AtomicIOSessionFactory.getInstance(dbConfig),
                    dbConfig,
                    kBinaryObjectFactory
            );
        }

        if (!column.isUnique()) {
            throw new SchemaException(DbError.INCOMPATIBLE_TYPE_FOR_INDEX_ERROR, String.format("Type %s used for column %s is not usable for index", column.getName(), column.getSqlType().getType()));
        }

        Column clusterColumn = SchemaSearcher.findClusterColumn(table);

        Serializer<V> clusterSerializer = SerializerRegistry.getInstance().getSerializer(clusterColumn.getSqlType().getType());

        BinaryObjectFactory<K> kBinaryObjectFactory = serializer.getBinaryObjectFactory(column);
        BinaryObjectFactory<V> vBinaryObjectFactory = clusterSerializer.getBinaryObjectFactory(clusterColumn);

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
