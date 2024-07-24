package org.elece.sql.parser.command;

import org.elece.sql.parser.StatementWrapper;
import org.elece.sql.parser.error.SqlException;
import org.elece.sql.parser.statement.TransactionStatement;
import org.elece.sql.token.IPeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.error.TokenizerException;
import org.elece.sql.token.model.KeywordToken;
import org.elece.sql.token.model.Token;
import org.elece.sql.token.model.type.Keyword;

public class StartKeywordCommand extends AbstractIKeywordCommand {
    public StartKeywordCommand(IPeekableIterator<TokenWrapper> tokenizer) {
        super(tokenizer);
    }

    @Override
    public StatementWrapper parse() throws SqlException, TokenizerException {
        expectToken(token -> token.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) token).getKeyword() == Keyword.Transaction);

        return StatementWrapper.builder().statement(new TransactionStatement()).build();
    }
}
