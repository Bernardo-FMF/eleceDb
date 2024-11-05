package org.elece.query.result;

import org.elece.db.schema.model.Table;

public class InsertResultInfo extends ResultInfo {
    private static final String PREFIX = "Response::InsertResult::";

    private final Table table;
    private final Integer rowCount;

    public InsertResultInfo(Table table, Integer rowCount) {
        this.table = table;
        this.rowCount = rowCount;
    }

    @Override
    public String deserialize() {
        StringBuilder innerData = new StringBuilder();
        innerData.append(table.toString()).append("\n")
                .append("RowCount: ").append(rowCount).append("\n");

        StringBuilder fullData = new StringBuilder();
        fullData.append(PREFIX)
                .append(innerData.length())
                .append("::\n")
                .append(innerData);

        return fullData.toString();
    }
}
