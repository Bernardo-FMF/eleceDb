package org.elece.query.plan.step.stream;

import org.elece.exception.proto.TcpException;
import org.elece.query.result.ResultInfo;

public abstract class StreamStep {
    public abstract void stream(byte[] data) throws TcpException;

    public abstract void stream(ResultInfo resultInfo) throws TcpException;
}
