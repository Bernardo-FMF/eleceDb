package org.elece.query.plan.step.tracer;

import org.elece.query.result.ResultInfo;

public abstract class TracerStep<V> {
    public abstract void trace(V value);

    public abstract ResultInfo buildResultInfo();
}
