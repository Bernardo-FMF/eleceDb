package org.elece.sql.token.model.type;

public enum Keyword implements IOperator {
    Select(true, false),
    Create(true, false),
    Update(true, false),
    Delete(true, false),
    Insert(true, false),
    Into(false, false),
    Values(false, false),
    Set(false, false),
    Drop(true, false),
    From(false, false),
    Where(false, false),
    And(false, false),
    Or(false, false),
    Primary(false, false),
    Key(false, false),
    Unique(false, false),
    Table(false, false),
    Database(false, false),
    Int(false, true),
    Varchar(false, true),
    Bool(false, true),
    True(false, false),
    False(false, false),
    Order(false, false),
    By(false, false),
    Index(false, false),
    On(false, false),
    Start(true, false),
    Transaction(false, false),
    Rollback(true, false),
    Commit(true, false),
    Explain(true, false),
    None(false, false);

    public static final Keyword[] VALUES = values();

    private final boolean isSupportedStatement;
    private final boolean isDataType;

    Keyword(boolean isSupportedStatement, boolean isDataType) {
        this.isSupportedStatement = isSupportedStatement;
        this.isDataType = isDataType;
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

    public boolean isDataType() {
        return isDataType;
    }
}
