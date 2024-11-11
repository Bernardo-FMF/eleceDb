package org.elece.query.result;

import org.elece.db.schema.model.Table;

public class DeleteResultInfo extends ResultInfo {
    private static final String PREFIX = "Response::DeleteResult::";

    private final Table table;
    private final ScanInfo scanInfo;
    private final Integer rowCount;

    public DeleteResultInfo(Table table, ScanInfo scanInfo, Integer rowCount) {
        this.table = table;
        this.scanInfo = scanInfo;
        this.rowCount = rowCount;
    }

    @Override
    public String deserialize() {
        StringBuilder innerData = new StringBuilder();
        innerData.append(table.toString()).append("\n")
                .append("RowCount: ").append(rowCount).append("\n")
                .append("ScanInfo: ").append(scanInfo.toString()).append("\n");

        StringBuilder fullData = new StringBuilder();
        fullData.append(PREFIX)
                .append(innerData.length())
                .append("::\n")
                .append(innerData);

        return fullData.toString();
    }
}
