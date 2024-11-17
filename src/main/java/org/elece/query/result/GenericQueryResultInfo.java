package org.elece.query.result;

import java.util.Objects;

public class GenericQueryResultInfo extends ResultInfo {
    private static final String PREFIX = "Response::%s::";

    private final QueryType queryType;
    private final String message;
    private final Integer rowCount;

    public GenericQueryResultInfo(QueryType queryType, Integer rowCount) {
        this(queryType, null, rowCount);
    }

    public GenericQueryResultInfo(QueryType queryType, String message, Integer rowCount) {
        this.queryType = queryType;
        this.message = message;
        this.rowCount = rowCount;
    }

    @Override
    public String deserialize() {
        StringBuilder innerData = new StringBuilder();
        if (Objects.nonNull(message)) {
            innerData.append("Message: ").append(message).append("\n");
        }
        innerData.append("RowCount: ").append(rowCount).append("\n");

        return String.format(PREFIX, queryType.getQueryHeader()) + innerData.length() + "::\n" + innerData;
    }

    public enum QueryType {
        CREATE_DB("CreateDbResult"),
        CREATE_INDEX("CreateIndexResult"),
        CREATE_TABLE("CreateTableResult"),
        DROP_DB("DropDbResult"),
        DROP_TABLE("DropTableResult"),
        DELETE("DeleteResult"),
        INSERT("InsertResult"),
        UPDATE("UpdateResult"),
        SELECT_END("SelectEndResult");

        private final String queryHeader;

        QueryType(String queryHeader) {
            this.queryHeader = queryHeader;
        }

        public String getQueryHeader() {
            return this.queryHeader;
        }
    }
}
