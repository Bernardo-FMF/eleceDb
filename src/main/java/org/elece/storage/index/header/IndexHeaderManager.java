package org.elece.storage.index.header;

import org.elece.exception.StorageException;
import org.elece.memory.Pointer;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public interface IndexHeaderManager {
    Optional<Location> getRootOfIndex(int indexId);

    void setRootOfIndex(int indexId, Location location) throws StorageException;

    void setIndexBeginningInChunk(int indexId, Location location) throws StorageException;

    Optional<Location> getIndexBeginningInChunk(int indexId, int chunk);

    Optional<Location> getNextIndexBeginningInChunk(int indexId, int chunk);

    List<Integer> getIndexesInChunk(int chunk);

    Optional<Integer> getNextIndexIdInChunk(int indexId, int chunk);

    List<Integer> getChunksOfIndex(int indexId);

    record Location(int chunk, long offset) {
        public static Location fromPointer(Pointer pointer) {
            return new Location(pointer.getChunk(), pointer.getPosition());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof Location(int thatChunk, long thatOffset)) {
                return this.chunk == thatChunk && this.offset == thatOffset;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(chunk(), offset());
        }
    }
}
