package org.elece.sql.parser.command;

import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.builder.ColumnBuilder;
import org.elece.exception.DbError;
import org.elece.exception.ParserException;
import org.elece.exception.TokenizerException;
import org.elece.sql.parser.expression.*;
import org.elece.sql.parser.expression.internal.*;
import org.elece.sql.token.PeekableIterator;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.model.*;
import org.elece.sql.token.model.type.Keyword;
import org.elece.sql.token.model.type.Symbol;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public abstract class AbstractKeywordCommand implements KeywordCommand {
    public static final String UNEXPECTED_TOKEN_PREFIX = "Unexpected token %s - %s";
    private final PeekableIterator<TokenWrapper> tokenizer;

    protected AbstractKeywordCommand(PeekableIterator<TokenWrapper> tokenizer) {
        this.tokenizer = tokenizer;
    }

    protected List<Expression> parseExpressionDefinitions(Boolean requiresParenthesis) throws ParserException,
                                                                                              TokenizerException {
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
        expectSymbolToken(Symbol.EQ);
        Expression expression = parseExpression();

        return new Assignment(identifier, expression);
    }

    protected <T> List<T> parseCommaSeparated(ParserFunction<T> parserFunction, Boolean requiresParenthesis) throws
                                                                                                             ParserException,
                                                                                                             TokenizerException {
        if (requiresParenthesis) {
            expectSymbolToken(Symbol.LEFT_PARENTHESIS);
        }

        List<T> results = new ArrayList<>();
        T parsedExpression = parserFunction.parse();
        results.add(parsedExpression);

        while (expectOptionalSymbolToken(Symbol.COMMA)) {
            results.add(parserFunction.parse());
        }

        if (requiresParenthesis) {
            expectSymbolToken(Symbol.RIGHT_PARENTHESIS);
        }

        return results;
    }

    protected List<Expression> parseOrderBy() throws TokenizerException, ParserException {
        if (expectOptionalKeywordToken(Keyword.ORDER)) {
            expectKeywordToken(Keyword.BY);
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
        if (expectOptionalKeywordToken(Keyword.WHERE)) {
            return parseExpression();
        }
        return null;
    }

    protected String parseIdentifier() throws TokenizerException, ParserException {
        Token nextToken = nextToken();
        if (nextToken.getTokenType() == Token.TokenType.IDENTIFIER_TOKEN) {
            return ((IdentifierToken) nextToken).getIdentifier();
        }
        throw new ParserException(DbError.UNEXPECTED_TOKEN_ERROR, String.format(UNEXPECTED_TOKEN_PREFIX, nextToken, "Expected an identifier"));
    }

    protected Column parseColumn() throws TokenizerException, ParserException {
        String name = parseIdentifier();
        Token nextToken = expectToken(token -> token.getTokenType() == Token.TokenType.KEYWORD_TOKEN && ((KeywordToken) token).getKeyword().isDataType());
        SqlType sqlType = switch (((KeywordToken) nextToken).getKeyword()) {
            case INT -> SqlType.intType;
            case BOOL -> SqlType.boolType;
            case VARCHAR -> {
                expectSymbolToken(Symbol.LEFT_PARENTHESIS);

                Token varcharToken = nextToken();
                if (varcharToken.getTokenType() != Token.TokenType.NUMBER_TOKEN) {
                    throw new ParserException(DbError.UNEXPECTED_TOKEN_ERROR, String.format(UNEXPECTED_TOKEN_PREFIX, varcharToken, "Expected a number corresponding to the varchar size"));
                }
                int size = Integer.parseInt(((NumberToken) varcharToken).getNumber());

                expectSymbolToken(Symbol.RIGHT_PARENTHESIS);
                yield SqlType.varchar(size);
            }
            default ->
                    throw new ParserException(DbError.UNEXPECTED_TOKEN_ERROR, String.format(UNEXPECTED_TOKEN_PREFIX, nextToken, "Token is invalid in the query context"));
        };

        List<SqlConstraint> columnCapabilities = new ArrayList<>();

        Token capabilityToken;
        while ((capabilityToken = peekToken()).getTokenType() == Token.TokenType.KEYWORD_TOKEN &&
                (((KeywordToken) capabilityToken).getKeyword() == Keyword.PRIMARY ||
                        ((KeywordToken) capabilityToken).getKeyword() == Keyword.UNIQUE)) {
            KeywordToken keywordToken = (KeywordToken) nextToken();
            if (keywordToken.getKeyword() == Keyword.PRIMARY) {
                expectKeywordToken(Keyword.KEY);

                columnCapabilities.add(SqlConstraint.PRIMARY_KEY);
            } else if (keywordToken.getKeyword() == Keyword.UNIQUE) {
                columnCapabilities.add(SqlConstraint.UNIQUE);
            } else {
                throw new ParserException(DbError.UNEXPECTED_TOKEN_ERROR, String.format(UNEXPECTED_TOKEN_PREFIX, capabilityToken, "Token is invalid in the query context"));
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
    public Expression parseInfix(Expression expression, Integer nextPrecedence) throws ParserException,
                                                                                       TokenizerException {
        Token nextToken = nextToken();
        if ((nextToken.getTokenType() == Token.TokenType.SYMBOL_TOKEN && ((SymbolToken) nextToken).getSymbol().isBinaryOperator())) {
            return new BinaryExpression(expression, ((SymbolToken) nextToken).getSymbol(), parseExpression(nextPrecedence));
        }
        if (nextToken.getTokenType() == Token.TokenType.KEYWORD_TOKEN && ((KeywordToken) nextToken).getKeyword().isBinaryOperator()) {
            return new BinaryExpression(expression, ((KeywordToken) nextToken).getKeyword(), parseExpression(nextPrecedence));
        }
        throw new ParserException(DbError.UNEXPECTED_TOKEN_ERROR, String.format(UNEXPECTED_TOKEN_PREFIX, nextToken, "Token is invalid in the query context"));
    }

    @Override
    public Expression parsePrefix() throws ParserException, TokenizerException {
        Token nextToken = nextToken();
        return switch (nextToken.getTokenType()) {
            case IDENTIFIER_TOKEN -> {
                Token possibleOrderByToken = peekToken();
                boolean isOrderBy = possibleOrderByToken.getTokenType() == Token.TokenType.KEYWORD_TOKEN &&
                        (((KeywordToken) possibleOrderByToken).getKeyword() == Keyword.DESC || ((KeywordToken) possibleOrderByToken).getKeyword() == Keyword.ASC);
                if (isOrderBy) {
                    Token order = nextToken();
                    yield new OrderIdentifierExpression(((IdentifierToken) nextToken).getIdentifier(), Order.fromKeyword(((KeywordToken) order).getKeyword()));
                }
                yield new IdentifierExpression(((IdentifierToken) nextToken).getIdentifier());
            }
            case STRING_TOKEN -> new ValueExpression<>(new SqlStringValue(((StringToken) nextToken).getString()));
            case NUMBER_TOKEN -> {
                String number = ((NumberToken) nextToken).getNumber();
                int value;
                try {
                    value = Integer.parseInt(number);
                } catch (NumberFormatException exception) {
                    throw new ParserException(DbError.INTEGER_OUT_OF_BOUNDS_ERROR, String.format("Integer %s is out of bounds", number));
                }
                yield new ValueExpression<>(new SqlNumberValue(value));
            }
            case KEYWORD_TOKEN -> switch (((KeywordToken) nextToken).getKeyword()) {
                case TRUE -> new ValueExpression<>(new SqlBoolValue(true));
                case FALSE -> new ValueExpression<>(new SqlBoolValue(false));
                default ->
                        throw new ParserException(DbError.UNEXPECTED_TOKEN_ERROR, String.format(UNEXPECTED_TOKEN_PREFIX, nextToken, "Token is invalid in the query context"));
            };
            case SYMBOL_TOKEN -> switch (((SymbolToken) nextToken).getSymbol()) {
                case MUL -> new WildcardExpression();
                case MINUS, PLUS -> new UnaryExpression(((SymbolToken) nextToken).getSymbol(), parseExpression(50));
                case LEFT_PARENTHESIS -> {
                    Expression expression = parseExpression();
                    expectSymbolToken(Symbol.RIGHT_PARENTHESIS);

                    yield new NestedExpression(expression);
                }
                default ->
                        throw new ParserException(DbError.UNEXPECTED_TOKEN_ERROR, String.format(UNEXPECTED_TOKEN_PREFIX, nextToken, "Token is invalid in the query context"));
            };
            default ->
                    throw new ParserException(DbError.UNEXPECTED_TOKEN_ERROR, String.format(UNEXPECTED_TOKEN_PREFIX, nextToken, "Token is invalid in the query context"));
        };
    }

    @Override
    public Integer getNextPrecedence() throws TokenizerException {
        Token nextToken = peekToken();
        Token.TokenType tokenType = nextToken.getTokenType();
        if (tokenType == Token.TokenType.KEYWORD_TOKEN) {
            Keyword keyword = ((KeywordToken) nextToken).getKeyword();
            return switch (keyword) {
                case OR -> 5;
                case AND -> 10;
                default -> 0;
            };
        } else if (tokenType == Token.TokenType.SYMBOL_TOKEN) {
            Symbol symbol = ((SymbolToken) nextToken).getSymbol();
            return switch (symbol) {
                case EQ, NEQ, GT, GT_EQ, LT, LT_EQ -> 20;
                case PLUS, MINUS -> 30;
                case MUL, DIV -> 40;
                default -> 0;
            };
        }

        return 0;
    }

    protected Token expectKeywordToken(Keyword keyword) throws TokenizerException, ParserException {
        return expectToken(token -> token.getTokenType() == Token.TokenType.KEYWORD_TOKEN && ((KeywordToken) token).getKeyword() == keyword);
    }

    protected Token expectSymbolToken(Symbol symbol) throws TokenizerException, ParserException {
        return expectToken(token -> token.getTokenType() == Token.TokenType.SYMBOL_TOKEN && ((SymbolToken) token).getSymbol() == symbol);
    }

    protected Boolean expectOptionalKeywordToken(Keyword keyword) throws TokenizerException {
        return expectOptionalToken(token -> token.getTokenType() == Token.TokenType.KEYWORD_TOKEN && ((KeywordToken) token).getKeyword() == keyword);
    }

    protected Boolean expectOptionalSymbolToken(Symbol symbol) throws TokenizerException {
        return expectOptionalToken(token -> token.getTokenType() == Token.TokenType.SYMBOL_TOKEN && ((SymbolToken) token).getSymbol() == symbol);
    }

    protected Token expectToken(Predicate<Token> predicate) throws TokenizerException, ParserException {
        Token target = nextToken();
        if (!predicate.test(target)) {
            throw new ParserException(DbError.UNEXPECTED_TOKEN_ERROR, String.format(UNEXPECTED_TOKEN_PREFIX, target, "Expected token not found"));
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
        while ((token = tokenizer.peek()).hasToken() && token.getToken().getTokenType() == Token.TokenType.WHITESPACE_TOKEN) {
            tokenizer.next();
        }
    }
}
