package org.elece.sql.error.type.parser;

import org.elece.sql.error.type.ISqlError;
import org.elece.sql.token.model.type.Symbol;

public class ArithmeticResultOutOfBounds implements ISqlError {
    private final Integer leftOperand;
    private final Integer rightOperand;
    private final Symbol operation;

    public ArithmeticResultOutOfBounds(Integer leftOperand, Integer rightOperand, Symbol operation) {
        this.leftOperand = leftOperand;
        this.rightOperand = rightOperand;
        this.operation = operation;
    }

    @Override
    public String format() {
        return format(String.format("Integer result of arithmetic operation is out of bounds %s %c %s", leftOperand, operation.getSymbolValue()[0], rightOperand), null);
    }
}
