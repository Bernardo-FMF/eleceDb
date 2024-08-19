package org.elece.storage.index;

import org.elece.memory.KvSize;
import org.elece.memory.Pointer;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface IndexStorageManager {
    CompletableFuture<Optional<NodeData>> getRoot(int indexId, KvSize KvSize) throws InterruptedException;

    byte[] getEmptyNode(KvSize KvSize);

    default CompletableFuture<NodeData> readNode(int indexId, Pointer pointer, KvSize KvSize) throws InterruptedException, IOException {
        return this.readNode(indexId, pointer.getPosition(), pointer.getChunk(), KvSize);
    }

    CompletableFuture<NodeData> readNode(int indexId, long position, int chunk, KvSize KvSize) throws InterruptedException, IOException;

    CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, boolean isRoot, KvSize size) throws IOException, ExecutionException, InterruptedException;

    CompletableFuture<Integer> updateNode(int indexId, byte[] data, Pointer pointer, boolean root) throws IOException, InterruptedException;

    void close() throws IOException;

    CompletableFuture<Integer> removeNode(int indexId, Pointer pointer, KvSize size) throws InterruptedException;

    boolean exists(int indexId);

    default boolean supportsPurge() {
        return false;
    }

    default void purgeIndex(int indexId) {
    }
}
