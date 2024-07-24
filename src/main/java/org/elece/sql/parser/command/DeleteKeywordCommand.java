package org.elece.sql.parser.command;

import org.elece.sql.parser.StatementWrapper;
import org.elece.sql.parser.error.SqlException;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.statement.DeleteStatement;
import org.elece.sql.token.IPeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.error.TokenizerException;
import org.elece.sql.token.model.KeywordToken;
import org.elece.sql.token.model.Token;
import org.elece.sql.token.model.type.Keyword;

public class DeleteKeywordCommand extends AbstractKeywordCommand {
    public DeleteKeywordCommand(IPeekableIterator<TokenWrapper> tokenizer) {
        super(tokenizer);
    }

    @Override
    public StatementWrapper parse() throws SqlException, TokenizerException {
        expectToken(token -> token.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) token).getKeyword() == Keyword.From);

        String identifier = parseIdentifier();
        Expression where = parseWhere();

        return StatementWrapper.builder().statement(new DeleteStatement(identifier, where)).build();
    }
}
