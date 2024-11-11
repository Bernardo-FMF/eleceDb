package org.elece.query.result.builder;

import org.elece.query.result.GenericQueryResultInfo;

public class GenericQueryResultInfoBuilder {
    private GenericQueryResultInfo.QueryType queryType;
    private Integer affectedRowCount;
    private String message;

    private GenericQueryResultInfoBuilder() {
        // private constructor
    }

    public static GenericQueryResultInfoBuilder builder() {
        return new GenericQueryResultInfoBuilder();
    }

    public GenericQueryResultInfoBuilder setQueryType(GenericQueryResultInfo.QueryType queryType) {
        this.queryType = queryType;
        return this;
    }

    public GenericQueryResultInfoBuilder setAffectedRowCount(Integer affectedRowCount) {
        this.affectedRowCount = affectedRowCount;
        return this;
    }

    public GenericQueryResultInfoBuilder setMessage(String message) {
        this.message = message;
        return this;
    }

    public GenericQueryResultInfo build() {
        return new GenericQueryResultInfo(queryType, message, affectedRowCount);
    }
}
