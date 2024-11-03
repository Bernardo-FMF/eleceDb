package org.elece.query.plan.step.stream;

import org.elece.query.result.ResultInfo;

public abstract class StreamStep {
    abstract void stream(byte[] data);

    abstract void stream(ResultInfo resultInfo);
}
