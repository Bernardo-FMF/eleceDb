package org.elece.query.plan.step.stream;

import org.elece.exception.ProtoException;
import org.elece.query.result.ResultInfo;

public abstract class StreamStep {
    public abstract void stream(byte[] data) throws ProtoException;

    public abstract void stream(ResultInfo resultInfo) throws ProtoException;
}
