package org.elece.query.result;

import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;

import java.util.List;

public class SelectResultInfo extends ResultInfo {
    private final List<Column> selectedColumns;
    private final Integer selectedColumnsSize;
    private final Table table;
    private final ScanInfo scanInfo;
    private final Integer rowCount;

    // TODO calculate the total size of the info
    public SelectResultInfo(List<Column> selectedColumns, Table table, ScanInfo scanInfo, Integer rowCount) {
        this.selectedColumns = selectedColumns;
        this.selectedColumnsSize = selectedColumns.stream().map(column -> column.getSqlType().getSize()).reduce(0, Integer::sum);
        this.table = table;
        this.scanInfo = scanInfo;
        this.rowCount = rowCount;
    }
}
