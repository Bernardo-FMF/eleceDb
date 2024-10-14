package org.elece.sql.parser.command;

import org.elece.exception.sql.ParserException;
import org.elece.exception.sql.TokenizerException;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.statement.InsertStatement;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.IPeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.SymbolToken;
import org.elece.sql.token.model.Token;
import org.elece.sql.token.model.type.Keyword;
import org.elece.sql.token.model.type.Symbol;

import java.util.ArrayList;
import java.util.List;

public class InsertKeywordCommand extends AbstractKeywordCommand {
    public InsertKeywordCommand(IPeekableIterator<TokenWrapper> tokenizer) {
        super(tokenizer);
    }

    @Override
    public Statement parse() throws ParserException, TokenizerException {
        expectKeywordToken(Keyword.Into);

        String table = parseIdentifier();

        Token parenthesisToken = peekToken();
        boolean hasColumnParenthesis = parenthesisToken.getTokenType() == Token.TokenType.SymbolToken && ((SymbolToken) parenthesisToken).getSymbol() == Symbol.LeftParenthesis;

        List<String> columns = new ArrayList<>();
        if (hasColumnParenthesis) {
            columns.addAll(parseIdentifierList());
        }

        expectKeywordToken(Keyword.Values);

        List<Expression> values = parseExpressionDefinitions(true);

        return new InsertStatement(table, columns, values);
    }
}
