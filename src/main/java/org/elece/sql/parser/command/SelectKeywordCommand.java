package org.elece.sql.parser.command;

import org.elece.sql.parser.StatementWrapper;
import org.elece.sql.parser.error.SqlException;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.statement.SelectStatement;
import org.elece.sql.token.IPeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.error.TokenizerException;
import org.elece.sql.token.model.KeywordToken;
import org.elece.sql.token.model.SymbolToken;
import org.elece.sql.token.model.Token;
import org.elece.sql.token.model.type.Keyword;
import org.elece.sql.token.model.type.Symbol;

import java.util.List;

public class SelectKeywordCommand extends AbstractIKeywordCommand {
    public SelectKeywordCommand(IPeekableIterator<TokenWrapper> tokenizer) {
        super(tokenizer);
    }

    @Override
    public StatementWrapper parse() throws SqlException, TokenizerException {
        List<Expression> selectedColumns = parseExpressionDefinitions(false);

        expectToken(token -> token.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) token).getKeyword() == Keyword.From);

        String identifier = parseIdentifier();
        Expression where = parseWhere();
        List<Expression> orderBy = parseOrderBy();

        expectToken(token -> token.getTokenType() == Token.TokenType.SymbolToken && ((SymbolToken) token).getSymbol() == Symbol.SemiColon);

        return StatementWrapper.builder().statement(new SelectStatement(selectedColumns, identifier, where, orderBy)).build();
    }
}
