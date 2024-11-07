package org.elece.sql.parser.command;

import org.elece.exception.ParserException;
import org.elece.exception.TokenizerException;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.statement.SelectStatement;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.PeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.type.Keyword;
import org.elece.sql.token.model.type.Symbol;

import java.util.List;

public class SelectKeywordCommand extends AbstractKeywordCommand {
    public SelectKeywordCommand(PeekableIterator<TokenWrapper> tokenizer) {
        super(tokenizer);
    }

    @Override
    public Statement parse() throws ParserException, TokenizerException {
        List<Expression> selectedColumns = parseExpressionDefinitions(false);

        expectKeywordToken(Keyword.From);

        String identifier = parseIdentifier();
        Expression where = parseWhere();
        List<Expression> orderBy = parseOrderBy();

        expectSymbolToken(Symbol.SemiColon);

        return new SelectStatement(selectedColumns, identifier, where, orderBy);
    }
}
