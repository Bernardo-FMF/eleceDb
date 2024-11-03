package org.elece.query.plan.step.filter;

import org.elece.db.DbObject;

import java.util.Optional;

public abstract class FilterStep {
    private final Long scanId;

    protected FilterStep(Long scanId) {
        this.scanId = scanId;
    }

    public Long getScanId() {
        return scanId;
    }

    abstract Optional<DbObject> next(DbObject dbObject);
}
