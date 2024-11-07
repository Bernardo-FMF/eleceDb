package org.elece.query.plan.step.stream;

import org.elece.exception.ProtoException;
import org.elece.query.result.ResultInfo;
import org.elece.thread.ClientBridge;
import org.elece.utils.BinaryUtils;

public class OutputStreamStep extends StreamStep {
    private final ClientBridge clientBridge;

    public OutputStreamStep(ClientBridge clientBridge) {
        this.clientBridge = clientBridge;
    }

    @Override
    public void stream(byte[] data) throws ProtoException {
        byte[] concatenatedData = new byte[data.length + Integer.BYTES];
        byte[] sizeBytes = BinaryUtils.integerToBytes(data.length);

        BinaryUtils.copyBytes(sizeBytes, concatenatedData, 0, 0, Integer.BYTES);
        BinaryUtils.copyBytes(data, concatenatedData, 0, Integer.BYTES, data.length);

        clientBridge.send(concatenatedData);
    }

    @Override
    public void stream(ResultInfo resultInfo) throws ProtoException {
        clientBridge.send(BinaryUtils.stringToBytes(resultInfo.deserialize()));
    }
}
