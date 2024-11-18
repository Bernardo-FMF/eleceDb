package org.elece.sql.parser.command;

import org.elece.exception.DbError;
import org.elece.exception.ParserException;
import org.elece.exception.TokenizerException;
import org.elece.sql.parser.statement.CreateDbStatement;
import org.elece.sql.parser.statement.CreateIndexStatement;
import org.elece.sql.parser.statement.CreateTableStatement;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.PeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.KeywordToken;
import org.elece.sql.token.model.Token;
import org.elece.sql.token.model.type.Keyword;
import org.elece.sql.token.model.type.Symbol;

import java.util.Set;

public class CreateKeywordCommand extends AbstractKeywordCommand {
    private static final Set<Keyword> supportedCreateKeywords = Set.of(Keyword.DATABASE, Keyword.TABLE, Keyword.INDEX, Keyword.UNIQUE);

    public CreateKeywordCommand(PeekableIterator<TokenWrapper> tokenizer) {
        super(tokenizer);
    }

    @Override
    public Statement parse() throws ParserException, TokenizerException {
        KeywordToken nextToken = (KeywordToken) expectToken(token -> token.getTokenType() == Token.TokenType.KEYWORD_TOKEN &&
                supportedCreateKeywords.contains(((KeywordToken) token).getKeyword()));

        return switch (nextToken.getKeyword()) {
            case DATABASE -> new CreateDbStatement(parseIdentifier());
            case TABLE -> new CreateTableStatement(parseIdentifier(), parseColumnDefinitions());
            case UNIQUE, INDEX -> {
                boolean isUnique = nextToken.getKeyword() == Keyword.UNIQUE;
                if (isUnique) {
                    expectKeywordToken(Keyword.INDEX);
                }

                String name = parseIdentifier();
                expectKeywordToken(Keyword.ON);
                String table = parseIdentifier();

                expectSymbolToken(Symbol.LEFT_PARENTHESIS);
                String column = parseIdentifier();
                expectSymbolToken(Symbol.RIGHT_PARENTHESIS);

                yield new CreateIndexStatement(name, table, column, isUnique);
            }
            default -> throw new ParserException(DbError.UNSPECIFIED_ERROR, "Unknown create query");
        };
    }
}
