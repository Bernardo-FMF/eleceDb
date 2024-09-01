package org.elece.sql.parser.command;

import org.elece.exception.sql.ParserException;
import org.elece.exception.sql.TokenizerException;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.statement.DeleteStatement;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.IPeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.KeywordToken;
import org.elece.sql.token.model.Token;
import org.elece.sql.token.model.type.Keyword;

public class DeleteKeywordCommand extends AbstractKeywordCommand {
    public DeleteKeywordCommand(IPeekableIterator<TokenWrapper> tokenizer) {
        super(tokenizer);
    }

    @Override
    public Statement parse() throws ParserException, TokenizerException {
        expectToken(token -> token.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) token).getKeyword() == Keyword.From);

        String identifier = parseIdentifier();
        Expression where = parseWhere();

        return new DeleteStatement(identifier, where);
    }
}
