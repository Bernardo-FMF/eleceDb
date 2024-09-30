package org.elece.sql.db.schema;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import org.elece.config.DbConfig;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.schema.type.SchemaAlreadyExistsError;
import org.elece.exception.schema.type.SchemaDoesNotExistError;
import org.elece.exception.schema.type.SchemaPersistenceError;
import org.elece.exception.sql.type.analyzer.TableNotPresentError;
import org.elece.exception.storage.StorageException;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.IndexManager;
import org.elece.sql.db.schema.model.Column;
import org.elece.sql.db.schema.model.Index;
import org.elece.sql.db.schema.model.Schema;
import org.elece.sql.db.schema.model.Table;
import org.elece.sql.db.schema.model.builder.SchemaBuilder;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

// TODO: add functionality of persisting new schema (for when the db is created/deleted, a table is created/deleted, an index is created/deleted).
public class JsonSchemaManager implements SchemaManager {
    private Schema schema;
    private final DbConfig dbConfig;
    private final ColumnIndexManagerProvider columnIndexManagerProvider;
    private final Gson gson;
    private final AtomicInteger tableIndex;

    public JsonSchemaManager(DbConfig dbConfig, ColumnIndexManagerProvider columnIndexManagerProvider) throws SchemaException {
        this.dbConfig = dbConfig;
        this.columnIndexManagerProvider = columnIndexManagerProvider;
        this.gson = new GsonBuilder().serializeNulls().create();
        loadSchema();
        // TODO: add some schema validation

        tableIndex = new AtomicInteger(schema.getCollections().isEmpty() ? 0 : schema.getCollections().getLast().getId());
    }

    private void loadSchema() throws SchemaException {
        String schemePath = getSchemePath();
        if (!Files.exists(Path.of(schemePath))) {
            this.schema = null;
            return;
        }
        try {
            FileReader fileReader = new FileReader(schemePath);
            JsonReader jsonReader = new JsonReader(fileReader);
            this.schema = gson.fromJson(jsonReader, Schema.class);
            fileReader.close();
        } catch (IOException exception) {
            throw new SchemaException(new SchemaPersistenceError(exception.getMessage()));
        }
    }

    private String getSchemePath() {
        return Path.of(this.dbConfig.getBaseDbPath(), "schema.json").toString();
    }

    private void persistSchema() throws SchemaException {
        try {
            FileWriter fileWriter = new FileWriter(this.getSchemePath());
            gson.toJson(this.schema, fileWriter);
            fileWriter.close();
        } catch (IOException exception) {
            throw new SchemaException(new SchemaPersistenceError(exception.getMessage()));
        }
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public synchronized void createSchema(String dbName) throws SchemaException {
        if (!Objects.isNull(schema)) {
            throw new SchemaException(new SchemaAlreadyExistsError());
        }

        this.schema = SchemaBuilder.builder()
                .setDbName(dbName)
                .build();
        persistSchema();
    }

    @Override
    public synchronized void createTable(Table table) throws SchemaException {
        validateSchemaExists();

        int nextTableIndex = tableIndex.incrementAndGet();
        table.setId(nextTableIndex);

        schema.addTable(table);

        persistSchema();

        // TODO: create index manager for primary key
    }

    // TODO: Can return number of rows deleted
    @Override
    public synchronized void deleteTable(String tableName) throws SchemaException, IOException, ExecutionException, InterruptedException, StorageException {
        validateSchemaExists();

        Optional<Table> optionalTable = SchemaSearcher.findTable(schema, tableName);
        if (optionalTable.isEmpty()) {
            throw new SchemaException(new TableNotPresentError(tableName));
        }

        schema.removeTable(tableName);

        persistSchema();

        Table table = optionalTable.get();
        for (Column column : table.getColumns()) {
            IndexManager<?, ?> indexManager = columnIndexManagerProvider.getIndexManager(table, column);
            indexManager.purgeIndex();

            columnIndexManagerProvider.clearIndexManager(table, column);
        }
    }

    // TODO: Can return number of rows affected
    @Override
    public synchronized void createIndex(String tableName, Index index) throws SchemaException {
        validateSchemaExists();

        Optional<Table> optionalTable = SchemaSearcher.findTable(schema, tableName);
        if (optionalTable.isEmpty()) {
            throw new SchemaException(new TableNotPresentError(tableName));
        }

        Table table = optionalTable.get();
        table.addIndex(index);

        persistSchema();

        // TODO: create index manager and fill the index
        //       to fill the index, we can obtain the pk index manager and create an iterator method to obtain pointers for each object stored; then for each object
        //       we use a mask to obtain the value of the column we are creating the index for and use that value to add in the new index manager.
    }

    private void validateSchemaExists() throws SchemaException {
        if (Objects.isNull(schema)) {
            throw new SchemaException(new SchemaDoesNotExistError());
        }
    }
}
