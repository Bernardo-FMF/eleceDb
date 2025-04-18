package org.elece.query.plan.step.tracer;

import org.elece.db.DbObject;
import org.elece.db.schema.model.Column;
import org.elece.query.result.ResultInfo;
import org.elece.query.result.builder.RowDescriptionResultInfoBuilder;

import java.util.List;

/**
 * Represents a mock tracer. The purpose of this, is to send to the client an initial header with the selected columns in the case of select statements.
 */
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
        return RowDescriptionResultInfoBuilder.builder()
                .setSelectedColumns(selectedColumns)
                .build();
    }
}
