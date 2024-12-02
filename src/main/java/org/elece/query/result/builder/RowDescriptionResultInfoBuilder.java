package org.elece.query.result.builder;

import org.elece.db.schema.model.Column;
import org.elece.query.result.RowDescriptionResultInfo;

import java.util.List;

public class RowDescriptionResultInfoBuilder {
    private List<Column> selectedColumns;

    private RowDescriptionResultInfoBuilder() {
        // private constructor
    }

    public static RowDescriptionResultInfoBuilder builder() {
        return new RowDescriptionResultInfoBuilder();
    }

    public RowDescriptionResultInfoBuilder setSelectedColumns(List<Column> selectedColumns) {
        this.selectedColumns = selectedColumns;
        return this;
    }

    public RowDescriptionResultInfo build() {
        return new RowDescriptionResultInfo(selectedColumns);
    }
}