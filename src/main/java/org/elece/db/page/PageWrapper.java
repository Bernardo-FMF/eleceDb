package org.elece.db.page;

import java.util.concurrent.atomic.AtomicInteger;

public class PageWrapper {
    private final Page page;
    private final AtomicInteger refCount = new AtomicInteger(0);

    public PageWrapper(Page page) {
        this.page = page;
    }

    public void incrementRefCount() {
        refCount.incrementAndGet();
    }

    public void decrementRefCount() {
        refCount.decrementAndGet();
    }

    public Page getPage() {
        return page;
    }

    public int getRefCount() {
        return refCount.get();
    }
}
