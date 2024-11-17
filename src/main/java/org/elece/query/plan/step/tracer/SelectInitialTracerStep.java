package org.elece.query.plan.step.tracer;

import org.elece.db.DbObject;
import org.elece.db.schema.model.Column;
import org.elece.query.result.ResultInfo;
import org.elece.query.result.builder.SelectInitialResultInfoBuilder;

import java.util.List;

public class SelectInitialTracerStep extends TracerStep<DbObject> {
    private final List<Column> selectedColumns;

    public SelectInitialTracerStep(List<Column> selectedColumns) {
        this.selectedColumns = selectedColumns;
    }

    @Override
    public void trace(DbObject value) {
        // This tracer is just used for the initial header data
    }

    @Override
    public ResultInfo buildResultInfo() {
        return SelectInitialResultInfoBuilder.builder()
                .setSelectedColumns(selectedColumns)
                .build();
    }
}
