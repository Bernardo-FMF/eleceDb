package org.elece.sql.db.schema;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import org.elece.config.DbConfig;
import org.elece.sql.db.schema.model.Schema;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

// TODO: add functionality of persisting new schema (for when the db is created/deleted, a table is created/deleted, an index is created/deleted).
public class JsonSchemaManager implements SchemaManager {
    private Schema schema;
    private final DbConfig dbConfig;
    private final Gson gson;

    public JsonSchemaManager(DbConfig dbConfig) {
        this.dbConfig = dbConfig;
        this.gson = new GsonBuilder().serializeNulls().create();
        loadSchema();
        // TODO: add some schema validation
    }

    // TODO: throw proper exception
    private void loadSchema() {
        String schemePath = getSchemePath();
        if (!Files.exists(Path.of(schemePath))) {
            return;
        }
        try {
            FileReader fileReader = new FileReader(schemePath);
            JsonReader jsonReader = new JsonReader(fileReader);
            this.schema = gson.fromJson(jsonReader, Schema.class);
            fileReader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getSchemePath() {
        return Path.of(this.dbConfig.getBaseDbPath(), "schema.json").toString();
    }

    private void persistSchema() throws IOException {
        FileWriter fileWriter = new FileWriter(this.getSchemePath());
        gson.toJson(this.schema, fileWriter);
        fileWriter.close();
    }

    public Schema getSchema() {
        return schema;
    }
}
