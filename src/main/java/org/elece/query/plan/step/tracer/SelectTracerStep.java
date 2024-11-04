package org.elece.query.plan.step.tracer;

import org.elece.db.DbObject;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.query.result.ResultInfo;
import org.elece.query.result.ScanInfo;
import org.elece.query.result.builder.SelectResultInfoBuilder;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SelectTracerStep extends TracerStep {
    private final List<Column> selectedColumns;
    private final Table table;
    private final ScanInfo scanInfo;

    private final AtomicInteger rowCounter;

    public SelectTracerStep(List<Column> selectedColumns,
                            Table table,
                            ScanInfo scanInfo) {
        this.selectedColumns = selectedColumns;
        this.table = table;
        this.scanInfo = scanInfo;
        this.rowCounter = new AtomicInteger(0);
    }

    @Override
    public void trace(DbObject dbObject) {
        rowCounter.incrementAndGet();
    }

    @Override
    public ResultInfo buildResultInfo() {
        return SelectResultInfoBuilder.builder()
                .setSelectedColumns(selectedColumns)
                .setTable(table)
                .setScanInfo(scanInfo)
                .setRowCount(rowCounter.get())
                .build();
    }
}
