package org.elece.sql.analyzer;

import org.elece.sql.error.AnalyzerException;
import org.elece.sql.db.IContext;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.parser.statement.Statement;

public interface ISqlAnalyzer {
    void analyze(IContext<String, TableMetadata> context, Statement statement) throws AnalyzerException;
}
