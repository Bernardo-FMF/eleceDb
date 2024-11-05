package org.elece.query.plan.step.tracer;

import org.elece.db.schema.model.Table;
import org.elece.query.result.ResultInfo;
import org.elece.query.result.builder.InsertResultInfoBuilder;

import java.util.concurrent.atomic.AtomicInteger;

public class InsertTracerStep extends TracerStep<byte[]> {
    private final Table table;
    private final AtomicInteger rowCounter;

    public InsertTracerStep(Table table) {
        this.table = table;
        this.rowCounter = new AtomicInteger(0);
    }

    @Override
    public void trace(byte[] value) {
        rowCounter.incrementAndGet();
    }

    @Override
    public ResultInfo buildResultInfo() {
        return InsertResultInfoBuilder.builder()
                .setTable(table)
                .setRowCount(rowCounter.get())
                .build();
    }
}
