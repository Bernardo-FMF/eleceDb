package org.elece.sql.parser;

import org.elece.sql.parser.command.CommandFactory;
import org.elece.sql.parser.command.IKeywordCommand;
import org.elece.sql.parser.error.SqlException;
import org.elece.sql.parser.error.UnexpectedToken;
import org.elece.sql.parser.error.UnspecifiedError;
import org.elece.sql.parser.statement.ExplainStatement;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.IPeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.Tokenizer;
import org.elece.sql.token.error.TokenizerException;
import org.elece.sql.token.model.KeywordToken;
import org.elece.sql.token.model.Token;
import org.elece.sql.token.model.type.Keyword;

import java.util.Iterator;
import java.util.Objects;

public class SqlParser implements ISqlParser {
    private final IPeekableIterator<TokenWrapper> tokenizerStream;

    public SqlParser(String input) {
        this.tokenizerStream = new Tokenizer(input).tokenize();
    }

    private static final CommandFactory commandFactory = new CommandFactory();

    public Statement parse() throws SqlException, TokenizerException {
        Iterator<TokenWrapper> whitespaceSkipper = tokenizerStream.takeWhile(token1 -> token1.hasToken() && token1.getToken().getTokenType() == Token.TokenType.WhitespaceToken);
        Token nextToken = whitespaceSkipper.next().unwrap();

        if (nextToken.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) nextToken).getKeyword().isSupportedStatement()) {
            KeywordToken keywordToken = (KeywordToken) nextToken;
            if (keywordToken.getKeyword() == Keyword.Explain) {
                return new ExplainStatement(parse());
            } else {
                IKeywordCommand IKeywordCommand = commandFactory.buildCommand(keywordToken.getKeyword(), tokenizerStream);
                if (Objects.isNull(IKeywordCommand)) {
                    throw new SqlException(new UnspecifiedError("Unresolved keyword command"));
                } else {
                    return IKeywordCommand.parse();
                }
            }
        }
        throw new SqlException(new UnexpectedToken(nextToken, "Keyword is not a supported statement"));
    }
}
