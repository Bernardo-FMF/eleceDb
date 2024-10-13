package org.elece.index;

import org.elece.config.DbConfig;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.schema.type.TableWithNoPrimaryKeyError;
import org.elece.exception.sql.type.analyzer.ColumnNotPresentError;
import org.elece.exception.sql.type.analyzer.IncompatibleTypeForIndexError;
import org.elece.exception.storage.StorageException;
import org.elece.memory.data.BinaryObjectFactory;
import org.elece.memory.tree.node.DefaultNodeFactory;
import org.elece.serializer.Serializer;
import org.elece.serializer.SerializerRegistry;
import org.elece.sql.parser.expression.internal.SqlConstraint;
import org.elece.storage.index.IndexStorageManagerFactory;
import org.elece.storage.index.session.factory.AtomicIOSessionFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

public class DefaultColumnIndexManagerProvider extends ColumnIndexManagerProvider {
    private final Map<String, IndexManager<?, ?>> indexManagers = new HashMap<>();

    public DefaultColumnIndexManagerProvider(DbConfig dbConfig, IndexStorageManagerFactory indexStorageManagerFactory) {
        super(dbConfig, indexStorageManagerFactory);
    }

    @Override
    public IndexManager<?, ?> getClusterIndexManager(Table table) throws SchemaException, StorageException {
        Optional<Column> clusterColumn = SchemaSearcher.findClusterColumn(table);
        if (clusterColumn.isEmpty()) {
            throw new SchemaException(new TableWithNoPrimaryKeyError(table.getName()));
        }
        return getIndexManager(table, clusterColumn.get());
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

    private <K extends Comparable<K>, V extends Comparable<V>> IndexManager<K, ?> buildIndexManager(Table table, Column column) throws StorageException, SchemaException {
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

        if (!column.getConstraints().contains(SqlConstraint.PrimaryKey) && !column.getConstraints().contains(SqlConstraint.Unique)) {
            throw new SchemaException(new IncompatibleTypeForIndexError(CLUSTER_ID, column.getSqlType().getType()));
        }

        Optional<Column> optionalClusterColumn = SchemaSearcher.findClusterColumn(table);
        if (optionalClusterColumn.isEmpty()) {
            throw new SchemaException(new ColumnNotPresentError(CLUSTER_ID, table.getName()));
        }

        Serializer<V> clusterSerializer = SerializerRegistry.getInstance().getSerializer(optionalClusterColumn.get().getSqlType().getType());

        BinaryObjectFactory<K> kBinaryObjectFactory = serializer.getBinaryObjectFactory(column);
        BinaryObjectFactory<V> vBinaryObjectFactory = clusterSerializer.getBinaryObjectFactory(optionalClusterColumn.get());

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
