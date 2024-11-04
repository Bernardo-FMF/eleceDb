package org.elece.query.result;

public class GenericQueryResultInfo extends ResultInfo {
    private static final String PREFIX = "Response::%s::";

    private final QueryType queryType;
    private final Integer rowCount;

    public GenericQueryResultInfo(QueryType queryType, Integer rowCount) {
        this.queryType = queryType;
        this.rowCount = rowCount;
    }

    @Override
    public String deserialize() {
        StringBuilder innerData = new StringBuilder();
        innerData.append("Message: Database created").append("\n")
                .append("AffectedRowCount: ").append(rowCount).append("\n");

        StringBuilder fullData = new StringBuilder();
        fullData.append(String.format(PREFIX, queryType.getQueryType()))
                .append(innerData.length())
                .append("::\n")
                .append(innerData);

        return fullData.toString();
    }

    public enum QueryType {
        CREATE_DB("CreateDbResult"),
        CREATE_INDEX("CreateIndexResult"),
        CREATE_TABLE("CreateTableResult"),
        DROP_DB("DropDbResult"),
        DROP_TABLE("DropTableResult");

        private final String queryType;

        QueryType(String queryType) {
            this.queryType = queryType;
        }

        public String getQueryType() {
            return this.queryType;
        }
    }
}
