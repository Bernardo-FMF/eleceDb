package org.elece.query.result.builder;

import org.elece.db.schema.model.Column;
import org.elece.query.result.SelectInitialResultInfo;

import java.util.List;

public class SelectInitialResultInfoBuilder {
    private List<Column> selectedColumns;

    private SelectInitialResultInfoBuilder() {
        // private constructor
    }

    public static SelectInitialResultInfoBuilder builder() {
        return new SelectInitialResultInfoBuilder();
    }

    public SelectInitialResultInfoBuilder setSelectedColumns(List<Column> selectedColumns) {
        this.selectedColumns = selectedColumns;
        return this;
    }

    public SelectInitialResultInfo build() {
        return new SelectInitialResultInfo(selectedColumns);
    }
}