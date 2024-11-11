package org.elece.query.result;

public class SelectEndResultInfo extends ResultInfo {
    private static final String PREFIX = "Response::SelectEndResult::";

    private final Integer rowCount;

    public SelectEndResultInfo(Integer rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public String deserialize() {
        StringBuilder innerData = new StringBuilder();
        innerData.append("RowCount: ").append(rowCount).append("\n");

        StringBuilder fullData = new StringBuilder();
        fullData.append(PREFIX)
                .append(innerData.length())
                .append("::\n")
                .append(innerData);

        return fullData.toString();
    }
}
