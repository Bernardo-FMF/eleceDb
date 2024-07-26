package org.elece.sql.parser.command;

import org.elece.sql.parser.error.SqlException;
import org.elece.sql.parser.statement.CreateDbStatement;
import org.elece.sql.parser.statement.CreateIndexStatement;
import org.elece.sql.parser.statement.CreateTableStatement;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.IPeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.error.TokenizerException;
import org.elece.sql.token.model.KeywordToken;
import org.elece.sql.token.model.SymbolToken;
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
    public Statement parse() throws SqlException, TokenizerException {
        KeywordToken nextToken = (KeywordToken) expectToken(token -> token.getTokenType() == Token.TokenType.KeywordToken &&
                supportedCreateKeywords.contains(((KeywordToken) token).getKeyword()));

        return switch (nextToken.getKeyword()) {
            case Database -> new CreateDbStatement(parseIdentifier());
            case Table -> new CreateTableStatement(parseIdentifier(), parseColumnDefinitions());
            case Unique, Index -> {
                boolean isUnique = nextToken.getKeyword() == Keyword.Unique;
                if (isUnique) {
                    expectToken(token -> token.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) token).getKeyword() == Keyword.Index);
                }

                String name = parseIdentifier();
                expectToken(token -> token.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) token).getKeyword() == Keyword.On);
                String table = parseIdentifier();

                expectToken(token -> token.getTokenType() == Token.TokenType.SymbolToken && ((SymbolToken) token).getSymbol() == Symbol.LeftParenthesis);
                String column = parseIdentifier();
                expectToken(token -> token.getTokenType() == Token.TokenType.SymbolToken && ((SymbolToken) token).getSymbol() == Symbol.RightParenthesis);

                yield new CreateIndexStatement(name, table, column, isUnique);
            }
            //TODO Fix error handling
            default -> throw new SqlException("");
        };
    }
}
