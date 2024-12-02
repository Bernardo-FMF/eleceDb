package org.elece.query.result;

public abstract class ResultInfo {
    protected static final Integer ERROR_RESPONSE_TYPE = 0;
    protected static final Integer GENERIC_RESPONSE_TYPE = 1;
    protected static final Integer ROW_DESCRIPTION_RESPONSE_TYPE = 2;
    protected static final Integer ROW_RESPONSE_TYPE = 3;

    public abstract String deserialize();
}
