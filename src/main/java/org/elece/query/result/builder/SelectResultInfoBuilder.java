package org.elece.query.result.builder;

import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.query.result.ScanInfo;
import org.elece.query.result.SelectResultInfo;

import java.util.List;

public class SelectResultInfoBuilder {
    private ScanInfo scanInfo;
    private Table table;
    private List<Column> selectedColumns;
    private Integer rowCount;

    private SelectResultInfoBuilder() {
        // private constructor
    }

    public static SelectResultInfoBuilder builder() {
        return new SelectResultInfoBuilder();
    }

    public SelectResultInfoBuilder setScanInfo(ScanInfo scanInfo) {
        this.scanInfo = scanInfo;
        return this;
    }

    public SelectResultInfoBuilder setTable(Table table) {
        this.table = table;
        return this;
    }

    public SelectResultInfoBuilder setSelectedColumns(List<Column> selectedColumns) {
        this.selectedColumns = selectedColumns;
        return this;
    }

    public SelectResultInfoBuilder setRowCount(Integer rowCount) {
        this.rowCount = rowCount;
        return this;
    }

    public SelectResultInfo build() {
        return new SelectResultInfo(scanInfo, table, selectedColumns, rowCount);
    }
}