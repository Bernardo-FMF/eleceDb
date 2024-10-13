package org.elece.index;

import java.util.Iterator;

public interface LockableIterator<X> extends Iterator<X> {
    void lock();

    void unlock();
}