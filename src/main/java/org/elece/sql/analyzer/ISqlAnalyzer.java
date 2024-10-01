package org.elece.sql.analyzer;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.sql.AnalyzerException;
import org.elece.sql.parser.statement.Statement;

public interface ISqlAnalyzer {
    void analyze(SchemaManager schemaManager, Statement statement) throws AnalyzerException;
}
