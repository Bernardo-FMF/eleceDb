package org.elece.query.result;

import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;

import java.util.List;

public class SelectResultInfo extends ResultInfo {
    private final ScanInfo scanInfo;
    private final Table table;
    private final List<Column> selectedColumns;
    private final Integer rowCount;

    public SelectResultInfo(ScanInfo scanInfo, Table table, List<Column> selectedColumns, Integer rowCount) {
        this.scanInfo = scanInfo;
        this.table = table;
        this.selectedColumns = selectedColumns;
        this.rowCount = rowCount;
    }
}
