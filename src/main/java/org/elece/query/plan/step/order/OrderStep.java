package org.elece.query.plan.step.order;

import java.util.Iterator;

public abstract class OrderStep {
    public abstract void addToBuffer(byte[] data);

    public abstract void prepareBufferState();

    public abstract Iterator<byte[]> getIterator();

    public abstract void clearBuffer();
}
