package org.elece.query.result.builder;

import org.elece.query.result.SelectEndResultInfo;

public class SelectEndResultInfoBuilder {
    private Integer rowCount;

    private SelectEndResultInfoBuilder() {
        // private constructor
    }

    public static SelectEndResultInfoBuilder builder() {
        return new SelectEndResultInfoBuilder();
    }

    public SelectEndResultInfoBuilder setRowCount(Integer rowCount) {
        this.rowCount = rowCount;
        return this;
    }

    public SelectEndResultInfo build() {
        return new SelectEndResultInfo(rowCount);
    }
}
