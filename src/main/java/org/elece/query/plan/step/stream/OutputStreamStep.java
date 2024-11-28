package org.elece.query.plan.step.stream;

import org.elece.exception.ProtoException;
import org.elece.query.result.ResultInfo;
import org.elece.thread.ClientInterface;
import org.elece.utils.BinaryUtils;

/**
 * Represents the streaming of either results in most cases, and also the rows in the case of select queries.
 */
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
