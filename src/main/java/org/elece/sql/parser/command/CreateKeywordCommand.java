package org.elece.sql.parser.command;

import org.elece.exception.sql.ParserException;
import org.elece.exception.sql.TokenizerException;
import org.elece.exception.sql.type.parser.UnspecifiedError;
import org.elece.sql.parser.statement.CreateDbStatement;
import org.elece.sql.parser.statement.CreateIndexStatement;
import org.elece.sql.parser.statement.CreateTableStatement;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.IPeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.KeywordToken;
import org.elece.sql.token.model.Token;
import org.elece.sql.token.model.type.Keyword;
import org.elece.sql.token.model.type.Symbol;

import java.util.Set;

public class CreateKeywordCommand extends AbstractKeywordCommand {
    private static final Set<Keyword> supportedCreateKeywords = Set.of(Keyword.Database, Keyword.Table, Keyword.Index, Keyword.Unique);

    public CreateKeywordCommand(IPeekableIterator<TokenWrapper> tokenizer) {
        super(tokenizer);
    }

    @Override
    public Statement parse() throws ParserException, TokenizerException {
        KeywordToken nextToken = (KeywordToken) expectToken(token -> token.getTokenType() == Token.TokenType.KeywordToken &&
                supportedCreateKeywords.contains(((KeywordToken) token).getKeyword()));

        return switch (nextToken.getKeyword()) {
            case Database -> new CreateDbStatement(parseIdentifier());
            case Table -> new CreateTableStatement(parseIdentifier(), parseColumnDefinitions());
            case Unique, Index -> {
                boolean isUnique = nextToken.getKeyword() == Keyword.Unique;
                if (isUnique) {
                    expectKeywordToken(Keyword.Index);
                }

                String name = parseIdentifier();
                expectKeywordToken(Keyword.On);
                String table = parseIdentifier();

                expectSymbolToken(Symbol.LeftParenthesis);
                String column = parseIdentifier();
                expectSymbolToken(Symbol.RightParenthesis);

                yield new CreateIndexStatement(name, table, column, isUnique);
            }
            default -> throw new ParserException(new UnspecifiedError("Unknown create query"));
        };
    }
}
