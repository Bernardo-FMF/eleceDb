package org.elece.query.plan.step.tracer;

import org.elece.db.DbObject;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.query.result.ResultInfo;
import org.elece.query.result.ScanInfo;
import org.elece.query.result.builder.SelectInitialResultInfoBuilder;

import java.util.List;

public class SelectInitialTracerStep extends TracerStep<DbObject> {
    private final List<Column> selectedColumns;
    private final Table table;
    private final ScanInfo scanInfo;

    public SelectInitialTracerStep(List<Column> selectedColumns, Table table, ScanInfo scanInfo) {
        this.selectedColumns = selectedColumns;
        this.table = table;
        this.scanInfo = scanInfo;
    }

    @Override
    public void trace(DbObject value) {
        // This tracer is just used for the initial header data
    }

    @Override
    public ResultInfo buildResultInfo() {
        return SelectInitialResultInfoBuilder.builder()
                .setSelectedColumns(selectedColumns)
                .setTable(table)
                .setScanInfo(scanInfo)
                .build();
    }
}
