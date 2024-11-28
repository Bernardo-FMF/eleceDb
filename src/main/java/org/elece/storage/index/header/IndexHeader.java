package org.elece.storage.index.header;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class IndexHeader {
    private Map<Integer, IndexHeaderManager.Location> roots = new ConcurrentHashMap<>();
    private Map<Integer, TreeSet<IndexOffset>> chunkIndexOffsets = new ConcurrentHashMap<>();

    public IndexHeader() {
    }

    public IndexHeader(Map<Integer, IndexHeaderManager.Location> roots,
                       Map<Integer, TreeSet<IndexOffset>> chunkIndexOffsets) {
        this.roots = roots;
        this.chunkIndexOffsets = chunkIndexOffsets;
    }

    public Map<Integer, IndexHeaderManager.Location> getRoots() {
        return Collections.unmodifiableMap(roots);
    }

    public void addRoot(int indexId, IndexHeaderManager.Location location) {
        roots.put(indexId, location);
    }

    public Map<Integer, TreeSet<IndexOffset>> getChunkIndexOffsets() {
        return Collections.unmodifiableMap(chunkIndexOffsets);
    }

    public TreeSet<IndexOffset> getIndexOffsets(int chunk) {
        return this.chunkIndexOffsets.computeIfAbsent(chunk, _ -> new TreeSet<>(Comparator.comparingLong(IndexOffset::getOffset)));
    }

    public Optional<IndexOffset> getNextIndexOffset(int chunk, int indexId) {
        SortedSet<IndexOffset> indexOffsets = this.getIndexOffsets(chunk);

        return indexOffsets.stream()
                .dropWhile(offset -> offset.getIndexId() != indexId)
                .skip(1)
                .findFirst();
    }

    public static final class IndexOffset implements Comparable<IndexOffset> {
        private int indexId;
        private long offset;

        public IndexOffset(int indexId, long offset) {
            this.indexId = indexId;
            this.offset = offset;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (Objects.isNull(obj) || getClass() != obj.getClass()) {
                return false;
            }
            IndexOffset that = (IndexOffset) obj;
            return indexId == that.indexId && offset == that.offset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexId, offset);
        }

        public int getIndexId() {
            return indexId;
        }

        public long getOffset() {
            return offset;
        }

        public void setIndexId(int indexId) {
            this.indexId = indexId;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        @Override
        public int compareTo(IndexOffset obj) {
            return Long.compare(this.offset, obj.offset);
        }
    }
}
