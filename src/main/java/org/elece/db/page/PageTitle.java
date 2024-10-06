package org.elece.db.page;

import java.util.Objects;

public final class PageTitle {
    private final int chunk;
    private final int pageNumber;

    public PageTitle(int chunk, int pageNumber) {
        this.chunk = chunk;
        this.pageNumber = pageNumber;
    }

    public static PageTitle of(Page page) {
        return new PageTitle(page.getChunk(), page.getPageNumber());
    }

    public int getChunk() {
        return chunk;
    }

    public int getPageNumber() {
        return pageNumber;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PageTitle pageTitle = (PageTitle) obj;
        return chunk == pageTitle.chunk && pageNumber == pageTitle.pageNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunk, pageNumber);
    }
}