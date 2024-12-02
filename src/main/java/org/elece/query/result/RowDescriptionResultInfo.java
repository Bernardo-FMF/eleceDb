package org.elece.query.result;

import org.elece.db.schema.model.Column;

import java.util.List;

public class RowDescriptionResultInfo extends ResultInfo {
    private static final String PREFIX = String.format("%d::Response::RowDescriptionResult::", ROW_DESCRIPTION_RESPONSE_TYPE);

    private final List<Column> selectedColumns;
    private final Integer selectedColumnsSize;

    public RowDescriptionResultInfo(List<Column> selectedColumns) {
        this.selectedColumns = selectedColumns;
        this.selectedColumnsSize = selectedColumns.stream().map(column -> column.getSqlType().getSize()).reduce(0, Integer::sum);
    }

    @Override
    public String deserialize() {
        StringBuilder innerData = new StringBuilder();
        innerData.append("RowSize: ").append(selectedColumnsSize).append("\n")
                .append("SelectedColumns: ").append(selectedColumns.toString()).append("\n");

        return PREFIX + innerData.length() + "::\n" + innerData;
    }
}
