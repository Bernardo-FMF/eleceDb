package org.elece.sql.token;

public class Location {
    /**
     * Line number starting at 1.
     */
    private int line;

    /**
     * Column number starting at 1.
     */
    private int column;

    /**
     * Pointer that indicates the exact index of the current character in the input
     */
    private int pointer;

    public Location() {
        line = 1;
        column = 1;
        pointer = 0;
    }

    public void incrementLine() {
        line++;
    }

    public void incrementPointer() {
        pointer++;
    }

    public void resetColumn() {
        column = 1;
    }

    public void incrementColumn() {
        column++;
    }

    public int getPointer() {
        return pointer;
    }
}
