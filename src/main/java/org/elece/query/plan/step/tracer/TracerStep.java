package org.elece.query.plan.step.tracer;

import org.elece.db.DbObject;
import org.elece.query.result.ResultInfo;

public abstract class TracerStep {
    public abstract void trace(DbObject dbObject);

    public abstract ResultInfo buildResultInfo();
}
