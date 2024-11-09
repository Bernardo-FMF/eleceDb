package org.elece.query.plan.step.scan;

import org.elece.db.DbObject;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ScanStep {
    private static final AtomicLong idCounter = new AtomicLong(0);

    private boolean finished;
    private final Long scanId;

    protected ScanStep() {
        this.finished = false;
        this.scanId = idCounter.incrementAndGet();
    }

    public boolean isFinished() {
        return finished;
    }

    public void finish() {
        this.finished = true;
    }

    public Long getScanId() {
        return scanId;
    }

    public abstract Optional<DbObject> next();
}
