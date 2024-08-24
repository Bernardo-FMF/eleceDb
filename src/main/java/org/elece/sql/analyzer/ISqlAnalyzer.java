package org.elece.sql.analyzer;

import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.parser.statement.Statement;

public interface ISqlAnalyzer {
    void analyze(SchemaManager schemaManager, Statement statement) throws AnalyzerException;
}
