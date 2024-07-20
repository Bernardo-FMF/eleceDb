package org.elece.sql.parser;

import org.elece.sql.parser.statement.Statement;

public interface ISqlParser {
    Statement parse();
}
