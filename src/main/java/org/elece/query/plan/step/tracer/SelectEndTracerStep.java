package org.elece.query.plan.step.tracer;

import org.elece.db.DbObject;
import org.elece.query.result.ResultInfo;
import org.elece.query.result.builder.SelectEndResultInfoBuilder;

import java.util.concurrent.atomic.AtomicInteger;

public class SelectEndTracerStep extends TracerStep<DbObject> {
    private final AtomicInteger rowCounter;

    public SelectEndTracerStep() {
        this.rowCounter = new AtomicInteger(0);
    }

    @Override
    public void trace(DbObject value) {
        rowCounter.incrementAndGet();
    }

    @Override
    public ResultInfo buildResultInfo() {
        return SelectEndResultInfoBuilder.builder()
                .setRowCount(rowCounter.get())
                .build();
    }
}
