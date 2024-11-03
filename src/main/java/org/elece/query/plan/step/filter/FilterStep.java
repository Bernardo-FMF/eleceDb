package org.elece.query.plan.step.filter;

import org.elece.db.DbObject;

public abstract class FilterStep {
    private final Long scanId;

    public FilterStep(Long scanId) {
        this.scanId = scanId;
    }

    public Long getScanId() {
        return scanId;
    }

    public abstract boolean next(DbObject dbObject);
}
