package org.elece.sql.parser.command;

import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.builder.ColumnBuilder;
import org.elece.exception.sql.ParserException;
import org.elece.exception.sql.TokenizerException;
import org.elece.exception.sql.type.parser.IntegerOutOfBoundsError;
import org.elece.exception.sql.type.parser.UnexpectedTokenError;
import org.elece.sql.parser.expression.*;
import org.elece.sql.parser.expression.internal.*;
import org.elece.sql.token.IPeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.*;
import org.elece.sql.token.model.type.Keyword;
import org.elece.sql.token.model.type.Symbol;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public abstract class AbstractKeywordCommand implements KeywordCommand {
    private final IPeekableIterator<TokenWrapper> tokenizer;

    public AbstractKeywordCommand(IPeekableIterator<TokenWrapper> tokenizer) {
        this.tokenizer = tokenizer;
    }

    protected List<Expression> parseExpressionDefinitions(Boolean requiresParenthesis) throws ParserException, TokenizerException {
        return parseCommaSeparated(this::parseExpression, requiresParenthesis);
    }

    protected List<Column> parseColumnDefinitions() throws ParserException, TokenizerException {
        return parseCommaSeparated(this::parseColumn, true);
    }

    protected List<String> parseIdentifierList() throws ParserException, TokenizerException {
        return parseCommaSeparated(this::parseIdentifier, true);
    }

    protected Assignment parseAssignment() throws ParserException, TokenizerException {
        String identifier = parseIdentifier();
        expectSymbolToken(Symbol.Eq);
        Expression expression = parseExpression();

        return new Assignment(identifier, expression);
    }

    protected <T> List<T> parseCommaSeparated(ParserFunction<T> parserFunction, Boolean requiresParenthesis) throws ParserException, TokenizerException {
        if (requiresParenthesis) {
            expectSymbolToken(Symbol.LeftParenthesis);
        }

        List<T> results = new ArrayList<>();
        T parsedExpression = parserFunction.parse();
        results.add(parsedExpression);

        while (expectOptionalSymbolToken(Symbol.Comma)) {
            results.add(parserFunction.parse());
        }

        if (requiresParenthesis) {
            expectSymbolToken(Symbol.RightParenthesis);
        }

        return results;
    }

    protected List<Expression> parseOrderBy() throws TokenizerException, ParserException {
        if (expectOptionalKeywordToken(Keyword.Order)) {
            expectKeywordToken(Keyword.By);
            List<Expression> expressions = parseExpressionDefinitions(false);
            List<Expression> convertedExpressions = new ArrayList<>();
            for (Expression expression : expressions) {
                if (expression instanceof IdentifierExpression identifierExpression) {
                    convertedExpressions.add(new OrderIdentifierExpression(identifierExpression.getName(), Order.DEFAULT_ORDER));
                } else {
                    convertedExpressions.add(expression);
                }
            }
            return convertedExpressions;
        }
        return List.of();
    }

    protected Expression parseWhere() throws TokenizerException, ParserException {
        if (expectOptionalKeywordToken(Keyword.Where)) {
            return parseExpression();
        }
        return null;
    }

    protected String parseIdentifier() throws TokenizerException, ParserException {
        Token nextToken = nextToken();
        if (nextToken.getTokenType() == Token.TokenType.IdentifierToken) {
            return ((IdentifierToken) nextToken).getIdentifier();
        }
        throw new ParserException(new UnexpectedTokenError(nextToken, "Expected an identifier"));
    }

    protected Column parseColumn() throws TokenizerException, ParserException {
        String name = parseIdentifier();
        Token nextToken = expectToken(token -> token.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) token).getKeyword().isDataType());
        SqlType sqlType = switch (((KeywordToken) nextToken).getKeyword()) {
            case Int -> SqlType.intType;
            case Bool -> SqlType.boolType;
            case Varchar -> {
                expectSymbolToken(Symbol.LeftParenthesis);

                Token varcharToken = nextToken();
                if (varcharToken.getTokenType() != Token.TokenType.NumberToken) {
                    throw new ParserException(new UnexpectedTokenError(varcharToken, "Expected a number corresponding to the varchar size"));
                }
                int size = Integer.parseInt(((NumberToken) varcharToken).getNumber());

                expectSymbolToken(Symbol.RightParenthesis);
                yield SqlType.varchar(size);
            }
            default ->
                    throw new ParserException(new UnexpectedTokenError(nextToken, "Token is invalid in the query context"));
        };

        List<SqlConstraint> columnCapabilities = new ArrayList<>();

        Token capabilityToken;
        while ((capabilityToken = peekToken()).getTokenType() == Token.TokenType.KeywordToken &&
                (((KeywordToken) capabilityToken).getKeyword() == Keyword.Primary ||
                        ((KeywordToken) capabilityToken).getKeyword() == Keyword.Unique)) {
            KeywordToken keywordToken = (KeywordToken) nextToken();
            if (keywordToken.getKeyword() == Keyword.Primary) {
                expectKeywordToken(Keyword.Key);

                columnCapabilities.add(SqlConstraint.PrimaryKey);
            } else if (keywordToken.getKeyword() == Keyword.Unique) {
                columnCapabilities.add(SqlConstraint.Unique);
            } else {
                throw new ParserException(new UnexpectedTokenError(capabilityToken, "Token is invalid in the query context"));
            }
        }

        return ColumnBuilder.builder()
                .setName(name)
                .setSqlType(sqlType)
                .setConstraints(columnCapabilities)
                .build();
    }

    @Override
    public Expression parseExpression(Integer precedence) throws ParserException, TokenizerException {
        Expression expression = parsePrefix();
        Integer nextPrecedence = getNextPrecedence();

        while (precedence < nextPrecedence) {
            expression = parseInfix(expression, nextPrecedence);
            nextPrecedence = getNextPrecedence();
        }

        return expression;
    }

    @Override
    public Expression parseInfix(Expression expression, Integer nextPrecedence) throws ParserException, TokenizerException {
        Token nextToken = nextToken();
        if ((nextToken.getTokenType() == Token.TokenType.SymbolToken && ((SymbolToken) nextToken).getSymbol().isBinaryOperator())) {
            return new BinaryExpression(expression, ((SymbolToken) nextToken).getSymbol(), parseExpression(nextPrecedence));
        }
        if (nextToken.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) nextToken).getKeyword().isBinaryOperator()) {
            return new BinaryExpression(expression, ((KeywordToken) nextToken).getKeyword(), parseExpression(nextPrecedence));
        }
        throw new ParserException(new UnexpectedTokenError(nextToken, "Token is invalid in the query context"));
    }

    @Override
    public Expression parsePrefix() throws ParserException, TokenizerException {
        Token nextToken = nextToken();
        return switch (nextToken.getTokenType()) {
            case IdentifierToken -> {
                Token possibleOrderByToken = peekToken();
                boolean isOrderBy = possibleOrderByToken.getTokenType() == Token.TokenType.KeywordToken &&
                        (((KeywordToken) possibleOrderByToken).getKeyword() == Keyword.Desc || ((KeywordToken) possibleOrderByToken).getKeyword() == Keyword.Asc);
                if (isOrderBy) {
                    Token order = nextToken();
                    yield new OrderIdentifierExpression(((IdentifierToken) nextToken).getIdentifier(), Order.fromKeyword(((KeywordToken) order).getKeyword()));
                }
                yield new IdentifierExpression(((IdentifierToken) nextToken).getIdentifier());
            }
            case StringToken -> new ValueExpression<>(new SqlStringValue(((StringToken) nextToken).getString()));
            case NumberToken -> {
                String number = ((NumberToken) nextToken).getNumber();
                int value;
                try {
                    value = Integer.parseInt(number);
                } catch (NumberFormatException exception) {
                    throw new ParserException(new IntegerOutOfBoundsError(number));
                }
                yield new ValueExpression<>(new SqlNumberValue(value));
            }
            case KeywordToken -> switch (((KeywordToken) nextToken).getKeyword()) {
                case True -> new ValueExpression<>(new SqlBoolValue(true));
                case False -> new ValueExpression<>(new SqlBoolValue(false));
                default ->
                        throw new ParserException(new UnexpectedTokenError(nextToken, "Token is invalid in the query context"));
            };
            case SymbolToken -> switch (((SymbolToken) nextToken).getSymbol()) {
                case Mul -> new WildcardExpression();
                case Minus, Plus -> new UnaryExpression(((SymbolToken) nextToken).getSymbol(), parseExpression(50));
                case LeftParenthesis -> {
                    Expression expression = parseExpression();
                    expectSymbolToken(Symbol.RightParenthesis);

                    yield new NestedExpression(expression);
                }
                default ->
                        throw new ParserException(new UnexpectedTokenError(nextToken, "Token is invalid in the query context"));
            };
            default ->
                    throw new ParserException(new UnexpectedTokenError(nextToken, "Token is invalid in the query context"));
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

    protected Token expectKeywordToken(Keyword keyword) throws TokenizerException, ParserException {
        return expectToken(token -> token.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) token).getKeyword() == keyword);
    }

    protected Token expectSymbolToken(Symbol symbol) throws TokenizerException, ParserException {
        return expectToken(token -> token.getTokenType() == Token.TokenType.SymbolToken && ((SymbolToken) token).getSymbol() == symbol);
    }

    protected Boolean expectOptionalKeywordToken(Keyword keyword) throws TokenizerException {
        return expectOptionalToken(token -> token.getTokenType() == Token.TokenType.KeywordToken && ((KeywordToken) token).getKeyword() == keyword);
    }

    protected Boolean expectOptionalSymbolToken(Symbol symbol) throws TokenizerException {
        return expectOptionalToken(token -> token.getTokenType() == Token.TokenType.SymbolToken && ((SymbolToken) token).getSymbol() == symbol);
    }

    protected Token expectToken(Predicate<Token> predicate) throws TokenizerException, ParserException {
        Token target = nextToken();
        if (!predicate.test(target)) {
            throw new ParserException(new UnexpectedTokenError(target, "Expected token not found"));
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
