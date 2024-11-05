package org.elece.query.result;

import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;

import java.util.List;

public class SelectResultInfo extends ResultInfo {
    private static final String PREFIX = "Response::SelectResult::";

    private final List<Column> selectedColumns;
    private final Integer selectedColumnsSize;
    private final Table table;
    private final ScanInfo scanInfo;
    private final Integer rowCount;

    public SelectResultInfo(List<Column> selectedColumns, Table table, ScanInfo scanInfo, Integer rowCount) {
        this.selectedColumns = selectedColumns;
        this.selectedColumnsSize = selectedColumns.stream().map(column -> column.getSqlType().getSize()).reduce(0, Integer::sum);
        this.table = table;
        this.scanInfo = scanInfo;
        this.rowCount = rowCount;
    }

    @Override
    public String deserialize() {
        StringBuilder innerData = new StringBuilder();
        innerData.append(table.toString()).append("\n")
                .append("RowCount: ").append(rowCount).append("\n")
                .append("RowSize: ").append(selectedColumnsSize).append("\n")
                .append("SelectedColumns: ").append(selectedColumns.toString()).append("\n")
                .append("ScanInfo: ").append(scanInfo.toString()).append("\n");

        StringBuilder fullData = new StringBuilder();
        fullData.append(PREFIX)
                .append(innerData.length())
                .append("::\n")
                .append(innerData);

        return fullData.toString();
    }
}
