package org.elece.sql.parser.command;

import org.elece.sql.parser.error.SqlException;
import org.elece.sql.parser.expression.*;
import org.elece.sql.parser.expression.internal.*;
import org.elece.sql.token.IPeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.error.TokenizerException;
import org.elece.sql.token.model.*;
import org.elece.sql.token.model.type.Keyword;
import org.elece.sql.token.model.type.Symbol;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public abstract class AbstractIKeywordCommand implements IKeywordCommand {
    private final IPeekableIterator<TokenWrapper> tokenizer;

    public AbstractIKeywordCommand(IPeekableIterator<TokenWrapper> tokenizer) {
        this.tokenizer = tokenizer;
    }

    protected List<Expression> parseExpressionDefinitions() throws SqlException, TokenizerException {
        return parseCommaSeparated(this::parseExpression, false);
    }


    protected List<Column> parseColumnDefinitions() throws SqlException, TokenizerException {
        return parseCommaSeparated(this::parseColumn, true);
    }

    protected List<String> parseIdentifierList() throws SqlException, TokenizerException {
        return parseCommaSeparated(this::parseIdentifier, true);
    }


    protected <T> List<T> parseCommaSeparated(IParserFunction<T> parserFunction, Boolean requiresParenthesis) throws SqlException, TokenizerException {
        if (requiresParenthesis) {
            expectToken(token -> token.getTokenType() == Token.TokenType.SymbolToken && ((SymbolToken) token).getSymbol() == Symbol.LeftParenthesis);
        }

        List<T> results = new ArrayList<>();
        T parsedExpression = parserFunction.parse();
        results.add(parsedExpression);

        while (expectOptionalToken(token -> token.getTokenType() == Token.TokenType.SymbolToken && ((SymbolToken) token).getSymbol() == Symbol.Comma)) {
            results.add(parserFunction.parse());
        }

        if (requiresParenthesis) {
            expectToken(token -> token.getTokenType() == Token.TokenType.SymbolToken && ((SymbolToken) token).getSymbol() == Symbol.RightParenthesis);
        }

        return results;
    }

    protected List<Expression> parseOrderBy() throws TokenizerException, SqlException {
        if (expectOptionalToken(token -> token.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) token).getKeyword() == Keyword.Order)) {
            expectToken(token -> token.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) token).getKeyword() == Keyword.By);
            return parseExpressionDefinitions();
        }
        return null;
    }

    protected Expression parseWhere() throws TokenizerException, SqlException {
        if (expectOptionalToken(token -> token.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) token).getKeyword() == Keyword.Where)) {
            return parseExpression();
        }
        return null;
    }

    protected String parseIdentifier() throws TokenizerException, SqlException {
        Token nextToken = nextToken();
        if (nextToken.getTokenType() == Token.TokenType.IdentifierToken) {
            return ((IdentifierToken) nextToken).getIdentifier();
        }
        throw new SqlException("");
    }

    protected Column parseColumn() throws TokenizerException, SqlException {
        String name = parseIdentifier();
        SqlType sqlType = switch (((KeywordToken) expectToken(token -> token.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) token).getKeyword().isDataType())).getKeyword()) {
            case Int -> {
                boolean unsigned = expectOptionalToken(token -> token.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) token).getKeyword() == Keyword.Unsigned);
                if (unsigned) {
                    yield SqlType.unsignedIntType;
                }
                yield SqlType.intType;
            }
            case BigInt -> {
                boolean unsigned = expectOptionalToken(token -> token.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) token).getKeyword() == Keyword.Unsigned);
                if (unsigned) {
                    yield SqlType.unsignedBigIntType;
                }
                yield SqlType.bigIntType;
            }
            case Bool -> SqlType.boolType;
            case Varchar -> {
                expectToken(token -> token.getTokenType() == Token.TokenType.SymbolToken && ((SymbolToken) token).getSymbol() == Symbol.LeftParenthesis);

                Token nextToken = nextToken();
                if (nextToken.getTokenType() != Token.TokenType.NumberToken) {
                    throw new SqlException("");
                }
                int size = Integer.parseInt(((NumberToken) nextToken).getNumber());

                expectToken(token -> token.getTokenType() == Token.TokenType.SymbolToken && ((SymbolToken) token).getSymbol() == Symbol.RightParenthesis);
                yield SqlType.varchar(size);
            }
            default -> throw new SqlException("");
        };

        List<SqlConstraint> columnCapabilities = new ArrayList<>();

        Token nextToken;
        while ((nextToken = peekToken()).getTokenType() == Token.TokenType.KeywordToken &&
                (((KeywordToken) nextToken).getKeyword() == Keyword.Primary ||
                        ((KeywordToken) nextToken).getKeyword() == Keyword.Unique)) {
            KeywordToken keywordToken = (KeywordToken) nextToken();
            if (keywordToken.getKeyword() == Keyword.Primary) {
                expectToken(token -> token.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) token).getKeyword() == Keyword.Key);

                columnCapabilities.add(SqlConstraint.PrimaryKey);
            } else if (keywordToken.getKeyword() == Keyword.Unique) {
                columnCapabilities.add(SqlConstraint.Unique);
            } else {
                throw new SqlException("");
            }
        }

        return new Column(name, sqlType, columnCapabilities);
    }

    @Override
    public Expression parseExpression(Integer precedence) throws SqlException, TokenizerException {
        Expression expression = parsePrefix();
        Integer nextPrecedence = getNextPrecedence();

        while (precedence < nextPrecedence) {
            expression = parseInfix(expression, nextPrecedence);
            nextPrecedence = getNextPrecedence();
        }

        return expression;
    }

    @Override
    public Expression parseInfix(Expression expression, Integer nextPrecedence) throws SqlException, TokenizerException {
        Token nextToken = nextToken();
        if ((nextToken.getTokenType() == Token.TokenType.SymbolToken && ((SymbolToken) nextToken).getSymbol().isBinaryOperator())) {
            return new BinaryExpression(expression, ((SymbolToken) nextToken).getSymbol(), parseExpression(nextPrecedence));
        }
        if (nextToken.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) nextToken).getKeyword().isBinaryOperator()) {
            return new BinaryExpression(expression, ((KeywordToken) nextToken).getKeyword(), parseExpression(nextPrecedence));
        }
        //TODO Fix error handling
        throw new SqlException("");
    }

    @Override
    public Expression parsePrefix() throws SqlException, TokenizerException {
        Token nextToken = nextToken();
        return switch (nextToken.getTokenType()) {
            case IdentifierToken -> new IdentifierExpression(((IdentifierToken) nextToken).getIdentifier());
            case StringToken -> new ValueExpression<>(new SqlStringValue(((StringToken) nextToken).getString()));
            case NumberToken ->
                    new ValueExpression<>(new SqlNumberValue(Long.parseLong(((NumberToken) nextToken).getNumber())));
            case KeywordToken -> switch (((KeywordToken) nextToken).getKeyword()) {
                case True -> new ValueExpression<>(new SqlBoolValue(true));
                case False -> new ValueExpression<>(new SqlBoolValue(false));
                //TODO Fix error handling
                default -> throw new SqlException("");
            };
            case SymbolToken -> switch (((SymbolToken) nextToken).getSymbol()) {
                case Mul -> new WildcardExpression();
                case Minus, Plus -> new UnaryExpression(((SymbolToken) nextToken).getSymbol(), parseExpression(50));
                case LeftParenthesis -> {
                    Expression expression = parseExpression();
                    expectToken(token1 -> token1.getTokenType() == Token.TokenType.SymbolToken && ((SymbolToken) token1).getSymbol() == Symbol.RightParenthesis);

                    yield new NestedExpression(expression);
                }
                //TODO Fix error handling
                default -> throw new SqlException("");
            };
            //TODO Fix error handling
            default -> throw new SqlException("");
        };
    }

    @Override
    public Integer getNextPrecedence() throws TokenizerException {
        Token nextToken = peekToken();
        Token.TokenType tokenType = nextToken.getTokenType();
        if (tokenType == Token.TokenType.KeywordToken) {
            Keyword keyword = ((KeywordToken) nextToken).getKeyword();
            return switch (keyword) {
                case Or -> 5;
                case And -> 10;
                default -> 0;
            };
        } else if (tokenType == Token.TokenType.SymbolToken) {
            Symbol symbol = ((SymbolToken) nextToken).getSymbol();
            return switch (symbol) {
                case Eq, Neq, Gt, GtEq, Lt, LtEq -> 20;
                case Plus, Minus -> 30;
                case Mul, Div -> 40;
                default -> 0;
            };
        }

        return 0;
    }

    protected Token expectToken(Predicate<Token> predicate) throws TokenizerException, SqlException {
        Token target = nextToken();
        if (!predicate.test(target)) {
            //TODO Fix error handling
            throw new SqlException("");
        }
        return target;
    }

    protected Boolean expectOptionalToken(Predicate<Token> predicate) throws TokenizerException {
        Token target = peekToken();
        if (predicate.test(target)) {
            nextToken();
            return true;
        }
        return false;
    }

    protected Token peekToken() throws TokenizerException {
        skipWhitespaceTokens();
        return tokenizer.peek().unwrap();
    }

    protected Token nextToken() throws TokenizerException {
        skipWhitespaceTokens();
        return tokenizer.next().unwrap();
    }

    protected void skipWhitespaceTokens() {
        TokenWrapper token;
        while ((token = tokenizer.peek()).hasToken() && token.getToken().getTokenType() == Token.TokenType.WhitespaceToken) {
            tokenizer.next();
        }
    }
}
