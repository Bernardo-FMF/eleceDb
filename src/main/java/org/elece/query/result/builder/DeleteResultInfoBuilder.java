package org.elece.query.result.builder;

import org.elece.db.schema.model.Table;
import org.elece.query.result.DeleteResultInfo;
import org.elece.query.result.ScanInfo;

public class DeleteResultInfoBuilder {
    private Table table;
    private ScanInfo scanInfo;
    private Integer rowCount;

    private DeleteResultInfoBuilder() {
        // private constructor
    }

    public static DeleteResultInfoBuilder builder() {
        return new DeleteResultInfoBuilder();
    }

    public DeleteResultInfoBuilder setTable(Table table) {
        this.table = table;
        return this;
    }

    public DeleteResultInfoBuilder setScanInfo(ScanInfo scanInfo) {
        this.scanInfo = scanInfo;
        return this;
    }

    public DeleteResultInfoBuilder setRowCount(Integer rowCount) {
        this.rowCount = rowCount;
        return this;
    }

    public DeleteResultInfo build() {
        return new DeleteResultInfo(table, scanInfo, rowCount);
    }
}
