package org.elece.sql.token.model.type;

public enum Keyword implements IOperator {
    Select(true),
    Create(true),
    Update(true),
    Delete(true),
    Insert(true),
    Into(false),
    Values(false),
    Set(false),
    Drop(true),
    From(false),
    Where(false),
    And(false),
    Or(false),
    Primary(false),
    Key(false),
    Unique(false),
    Table(false),
    Database(false),
    Int(false),
    BigInt(false),
    Unsigned(false),
    Varchar(false),
    Bool(false),
    True(false),
    False(false),
    Order(false),
    By(false),
    Index(false),
    On(false),
    Start(true),
    Transaction(false),
    Rollback(true),
    Commit(true),
    Explain(true),
    None(false);

    public static final Keyword[] VALUES = values();

    private final boolean isSupportedStatement;

    Keyword(boolean isSupportedStatement) {
        this.isSupportedStatement = isSupportedStatement;
    }

    public static Keyword getKeyword(String possibleKeyword) {
        for (Keyword keyword : VALUES) {
            if (keyword.name().equalsIgnoreCase(possibleKeyword)) {
                return keyword;
            }
        }
        return Keyword.None;
    }

    @Override
    public boolean isBinaryOperator() {
        return this == Keyword.And || this == Keyword.Or;
    }

    @Override
    public boolean isUnaryOperator() {
        return false;
    }

    public boolean isSupportedStatement() {
        return isSupportedStatement;
    }
}
