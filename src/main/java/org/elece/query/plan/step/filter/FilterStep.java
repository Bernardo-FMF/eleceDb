package org.elece.query.plan.step.filter;

import org.elece.db.DbObject;
import org.elece.exception.DeserializationException;
import org.elece.exception.ParserException;

public abstract class FilterStep {
    private final Long scanId;

    protected FilterStep(Long scanId) {
        this.scanId = scanId;
    }

    public Long getScanId() {
        return scanId;
    }

    public abstract boolean next(DbObject dbObject) throws ParserException, DeserializationException;
}
