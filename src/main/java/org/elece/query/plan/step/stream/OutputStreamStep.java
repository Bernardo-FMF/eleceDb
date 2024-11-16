package org.elece.query.plan.step.stream;

import org.elece.exception.ProtoException;
import org.elece.query.result.ResultInfo;
import org.elece.thread.ClientInterface;
import org.elece.utils.BinaryUtils;

public class OutputStreamStep extends StreamStep {
    private final ClientInterface clientInterface;

    public OutputStreamStep(ClientInterface clientInterface) {
        this.clientInterface = clientInterface;
    }

    @Override
    public void stream(byte[] data) throws ProtoException {
        clientInterface.send(data);
    }

    @Override
    public void stream(ResultInfo resultInfo) throws ProtoException {
        clientInterface.send(BinaryUtils.stringToBytes(resultInfo.deserialize()));
    }
}
