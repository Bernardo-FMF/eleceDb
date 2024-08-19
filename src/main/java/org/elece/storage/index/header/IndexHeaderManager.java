package org.elece.storage.index.header;

import org.elece.memory.Pointer;
import org.elece.storage.error.StorageException;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public interface IndexHeaderManager {
    Optional<Location> getRootOfIndex(int indexId);

    void setRootOfIndex(int indexId, Location location) throws StorageException;

    void setIndexBeginningInChunk(int indexId, Location location) throws StorageException;

    default Optional<Location> getIndexBeginningInChunk(int indexId, int chunk) {
        return Optional.of(new Location(0, 0));
    }

    default Optional<Location> getNextIndexBeginningInChunk(int indexId, int chunk) {
        return Optional.of(new Location(0, 0));
    }

    List<Integer> getIndexesInChunk(int chunk);

    Optional<Integer> getNextIndexIdInChunk(int indexId, int chunk);

    List<Integer> getChunksOfIndex(int indexId);

    record Location(int chunk, long offset) {
        public static Location fromPointer(Pointer pointer) {
            return new Location(pointer.getChunk(), pointer.getPosition());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Location location = (Location) o;
            return chunk() == location.chunk() && offset() == location.offset();
        }

        @Override
        public int hashCode() {
            return Objects.hash(chunk(), offset());
        }
    }
}
