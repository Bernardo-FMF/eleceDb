package org.elece.sql.parser;

import org.elece.exception.DbError;
import org.elece.exception.ParserException;
import org.elece.exception.TokenizerException;
import org.elece.sql.parser.command.CommandFactory;
import org.elece.sql.parser.command.KeywordCommand;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.DefaultTokenizer;
import org.elece.sql.token.PeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.KeywordToken;
import org.elece.sql.token.model.Token;

import java.util.Iterator;
import java.util.Objects;

public class SqlParser {
    private final PeekableIterator<TokenWrapper> tokenizerStream;

    public SqlParser(String input) {
        this.tokenizerStream = new DefaultTokenizer(input).tokenize();
    }

    private static final CommandFactory commandFactory = new CommandFactory();

    public Statement parse() throws ParserException, TokenizerException {
        Iterator<TokenWrapper> whitespaceSkipper = tokenizerStream.takeWhile(token1 -> token1.hasToken() && token1.getToken().getTokenType() == Token.TokenType.WHITESPACE_TOKEN);
        Token nextToken = whitespaceSkipper.next().unwrap();

        if (nextToken.getTokenType() == Token.TokenType.KEYWORD_TOKEN && ((KeywordToken) nextToken).getKeyword().isSupportedStatement()) {
            KeywordToken keywordToken = (KeywordToken) nextToken;
            KeywordCommand keywordCommand = commandFactory.buildCommand(keywordToken.getKeyword(), tokenizerStream);
            if (Objects.isNull(keywordCommand)) {
                throw new ParserException(DbError.UNSPECIFIED_ERROR, "Unresolved keyword command");
            } else {
                return keywordCommand.parse();
            }
        }
        throw new ParserException(DbError.UNEXPECTED_TOKEN_ERROR, String.format("Unexpected token %s - %s", nextToken, "Keyword is not a supported statement"));
    }
}
