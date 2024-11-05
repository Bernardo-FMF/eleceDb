package org.elece.query.plan.step.tracer;

import org.elece.db.DbObject;
import org.elece.db.schema.model.Table;
import org.elece.query.result.ResultInfo;
import org.elece.query.result.ScanInfo;
import org.elece.query.result.builder.UpdateResultInfoBuilder;

import java.util.concurrent.atomic.AtomicInteger;

public class UpdateTracerStep extends TracerStep<DbObject> {
    private final Table table;
    private final AtomicInteger rowCounter;
    private final ScanInfo scanInfo;

    public UpdateTracerStep(Table table, ScanInfo scanInfo) {
        this.table = table;
        this.scanInfo = scanInfo;
        this.rowCounter = new AtomicInteger(0);
    }

    @Override
    public void trace(DbObject value) {
        rowCounter.incrementAndGet();
    }

    @Override
    public ResultInfo buildResultInfo() {
        return UpdateResultInfoBuilder.builder()
                .setTable(table)
                .setScanInfo(scanInfo)
                .setRowCount(rowCounter.get())
                .build();
    }
}
