package org.elece.query.result;

public class RowResultInfo extends ResultInfo {
    private static final String PREFIX = String.format("%d::Response::RowResult::", ROW_RESPONSE_TYPE);

    private final String rowData;

    public RowResultInfo(String rowData) {
        this.rowData = rowData;
    }

    @Override
    public String deserialize() {
        StringBuilder innerData = new StringBuilder();
        innerData.append("Row: ").append(rowData).append("\n");

        return PREFIX + innerData.length() + "::\n" + innerData;
    }
}
