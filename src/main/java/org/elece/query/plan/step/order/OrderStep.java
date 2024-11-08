package org.elece.query.plan.step.order;

import org.elece.exception.FileChannelException;
import org.elece.exception.InterruptedTaskException;
import org.elece.exception.StorageException;

import java.util.Iterator;

public abstract class OrderStep {
    public abstract void addToBuffer(byte[] data) throws InterruptedTaskException, StorageException,
                                                         FileChannelException;

    public abstract void prepareBufferState() throws InterruptedTaskException, StorageException, FileChannelException;

    public abstract Iterator<byte[]> getIterator();

    public abstract void clearBuffer() throws InterruptedTaskException, StorageException, FileChannelException;
}
