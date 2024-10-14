package org.elece.sql.parser.command;

import org.elece.exception.sql.ParserException;
import org.elece.exception.sql.TokenizerException;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.expression.internal.Assignment;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.parser.statement.UpdateStatement;
import org.elece.sql.token.IPeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.type.Keyword;

import java.util.List;

public class UpdateKeywordCommand extends AbstractKeywordCommand {
    public UpdateKeywordCommand(IPeekableIterator<TokenWrapper> tokenizer) {
        super(tokenizer);
    }

    @Override
    public Statement parse() throws ParserException, TokenizerException {
        String table = parseIdentifier();

        expectKeywordToken(Keyword.Set);

        List<Assignment> assignments = parseCommaSeparated(this::parseAssignment, false);
        Expression where = parseWhere();

        return new UpdateStatement(table, assignments, where);
    }
}
