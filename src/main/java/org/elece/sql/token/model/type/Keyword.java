package org.elece.sql.token.model.type;

public enum Keyword implements IOperator {
    Select,
    Create,
    Update,
    Delete,
    Insert,
    Into,
    Values,
    Set,
    Drop,
    From,
    Where,
    And,
    Or,
    Primary,
    Key,
    Unique,
    Table,
    Database,
    Int,
    BigInt,
    Unsigned,
    Varchar,
    Bool,
    True,
    False,
    Order,
    By,
    Index,
    On,
    Start,
    Transaction,
    Rollback,
    Commit,
    Explain,
    None;

    public static final Keyword[] VALUES = values();

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
}
