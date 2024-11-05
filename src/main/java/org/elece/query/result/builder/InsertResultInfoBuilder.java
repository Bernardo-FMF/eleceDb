package org.elece.query.result.builder;

import org.elece.db.schema.model.Table;
import org.elece.query.result.InsertResultInfo;

public class InsertResultInfoBuilder {
    private Table table;
    private Integer rowCount;

    private InsertResultInfoBuilder() {
        // private constructor
    }

    public static InsertResultInfoBuilder builder() {
        return new InsertResultInfoBuilder();
    }

    public InsertResultInfoBuilder setTable(Table table) {
        this.table = table;
        return this;
    }

    public InsertResultInfoBuilder setRowCount(Integer rowCount) {
        this.rowCount = rowCount;
        return this;
    }

    public InsertResultInfo build() {
        return new InsertResultInfo(table, rowCount);
    }
}
