package org.elece.exception.sql.type.parser;

import org.elece.exception.DbError;
import org.elece.sql.token.model.type.Symbol;

public class ArithmeticResultOutOfBoundsError implements DbError {
    private final Integer leftOperand;
    private final Integer rightOperand;
    private final Symbol operation;

    public ArithmeticResultOutOfBoundsError(Integer leftOperand, Integer rightOperand, Symbol operation) {
        this.leftOperand = leftOperand;
        this.rightOperand = rightOperand;
        this.operation = operation;
    }

    @Override
    public String format() {
        return String.format("Integer result of arithmetic operation is out of bounds %s %c %s", leftOperand, operation.getSymbolValue()[0], rightOperand);
    }
}
