package org.elece.sql.parser.command;

import org.elece.sql.error.ParserException;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.parser.statement.TransactionStatement;
import org.elece.sql.token.IPeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.error.TokenizerException;
import org.elece.sql.token.model.KeywordToken;
import org.elece.sql.token.model.Token;
import org.elece.sql.token.model.type.Keyword;

public class StartKeywordCommand extends AbstractKeywordCommand {
    public StartKeywordCommand(IPeekableIterator<TokenWrapper> tokenizer) {
        super(tokenizer);
    }

    @Override
    public Statement parse() throws ParserException, TokenizerException {
        expectToken(token -> token.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) token).getKeyword() == Keyword.Transaction);

        return new TransactionStatement();
    }
}
