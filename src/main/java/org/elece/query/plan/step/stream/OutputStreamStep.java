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
        byte[] concatenatedData = new byte[data.length + Integer.BYTES];
        byte[] sizeBytes = BinaryUtils.integerToBytes(data.length);

        BinaryUtils.copyBytes(sizeBytes, concatenatedData, 0, 0, Integer.BYTES);
        BinaryUtils.copyBytes(data, concatenatedData, 0, Integer.BYTES, data.length);

        clientInterface.send(concatenatedData);
    }

    @Override
    public void stream(ResultInfo resultInfo) throws ProtoException {
        clientInterface.send(BinaryUtils.stringToBytes(resultInfo.deserialize()));
    }
}
