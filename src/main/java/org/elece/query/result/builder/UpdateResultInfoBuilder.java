package org.elece.query.result.builder;

import org.elece.db.schema.model.Table;
import org.elece.query.result.ScanInfo;
import org.elece.query.result.UpdateResultInfo;

public class UpdateResultInfoBuilder {
    private Table table;
    private ScanInfo scanInfo;
    private Integer rowCount;

    private UpdateResultInfoBuilder() {
        // private constructor
    }

    public static UpdateResultInfoBuilder builder() {
        return new UpdateResultInfoBuilder();
    }

    public UpdateResultInfoBuilder setTable(Table table) {
        this.table = table;
        return this;
    }

    public UpdateResultInfoBuilder setScanInfo(ScanInfo scanInfo) {
        this.scanInfo = scanInfo;
        return this;
    }

    public UpdateResultInfoBuilder setRowCount(Integer rowCount) {
        this.rowCount = rowCount;
        return this;
    }

    public UpdateResultInfo build() {
        return new UpdateResultInfo(table, scanInfo, rowCount);
    }
}
