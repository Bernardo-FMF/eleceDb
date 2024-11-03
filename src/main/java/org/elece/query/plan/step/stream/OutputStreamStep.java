package org.elece.query.plan.step.stream;

import org.elece.query.result.ResultInfo;
import org.elece.thread.ClientBridge;

public class OutputStreamStep extends StreamStep {
    private final ClientBridge clientBridge;

    public OutputStreamStep(ClientBridge clientBridge) {
        this.clientBridge = clientBridge;
    }

    @Override
    void stream(byte[] data) {
        // TODO need to implement
    }

    @Override
    void stream(ResultInfo resultInfo) {
        // TODO need to implement
    }
}
