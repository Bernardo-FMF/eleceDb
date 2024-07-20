package org.elece.sql.parser;

import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.ITokenizer;
import org.elece.sql.token.Tokenizer;

public class SqlParser implements ISqlParser {
    private final ITokenizer tokenizer;

    public SqlParser(String input) {
        this.tokenizer = new Tokenizer(input);
    }

    @Override
    public Statement parse() {
        return null;
    }
}
