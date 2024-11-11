package org.elece.query.result.builder;

import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.query.result.ScanInfo;
import org.elece.query.result.SelectInitialResultInfo;

import java.util.List;

public class SelectInitialResultInfoBuilder {
    private ScanInfo scanInfo;
    private Table table;
    private List<Column> selectedColumns;

    private SelectInitialResultInfoBuilder() {
        // private constructor
    }

    public static SelectInitialResultInfoBuilder builder() {
        return new SelectInitialResultInfoBuilder();
    }

    public SelectInitialResultInfoBuilder setScanInfo(ScanInfo scanInfo) {
        this.scanInfo = scanInfo;
        return this;
    }

    public SelectInitialResultInfoBuilder setTable(Table table) {
        this.table = table;
        return this;
    }

    public SelectInitialResultInfoBuilder setSelectedColumns(List<Column> selectedColumns) {
        this.selectedColumns = selectedColumns;
        return this;
    }

    public SelectInitialResultInfo build() {
        return new SelectInitialResultInfo(selectedColumns, table, scanInfo);
    }
}