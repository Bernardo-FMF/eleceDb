package org.elece.sql.token.model.type;

public enum Keyword implements IOperator {
    SELECT(true, false),
    CREATE(true, false),
    UPDATE(true, false),
    DELETE(true, false),
    INSERT(true, false),
    INTO(false, false),
    VALUES(false, false),
    SET(false, false),
    DROP(true, false),
    FROM(false, false),
    WHERE(false, false),
    AND(false, false),
    OR(false, false),
    PRIMARY(false, false),
    KEY(false, false),
    UNIQUE(false, false),
    TABLE(false, false),
    DATABASE(false, false),
    INT(false, true),
    VARCHAR(false, true),
    BOOL(false, true),
    TRUE(false, false),
    FALSE(false, false),
    ORDER(false, false),
    BY(false, false),
    INDEX(false, false),
    ON(false, false),
    DESC(false, false),
    ASC(false, false),
    NONE(false, false);

    private static final Keyword[] KEYWORDS = values();

    private final boolean isSupportedStatement;
    private final boolean isDataType;

    Keyword(boolean isSupportedStatement, boolean isDataType) {
        this.isSupportedStatement = isSupportedStatement;
        this.isDataType = isDataType;
    }

    public static Keyword getKeyword(String possibleKeyword) {
        for (Keyword keyword : KEYWORDS) {
            if (keyword.name().equalsIgnoreCase(possibleKeyword)) {
                return keyword;
            }
        }
        return Keyword.NONE;
    }

    @Override
    public boolean isBinaryOperator() {
        return this == Keyword.AND || this == Keyword.OR;
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
