package org.elece.storage.index;

import org.elece.exception.StorageException;
import org.elece.memory.KeyValueSize;
import org.elece.memory.Pointer;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface IndexStorageManager {
    CompletableFuture<Optional<NodeData>> getRoot(int indexId, KeyValueSize keyValueSize) throws InterruptedException, StorageException;

    byte[] getEmptyNode(KeyValueSize keyValueSize);

    default CompletableFuture<NodeData> readNode(int indexId, Pointer pointer, KeyValueSize keyValueSize) throws InterruptedException, IOException, StorageException {
        return this.readNode(indexId, pointer.getPosition(), pointer.getChunk(), keyValueSize);
    }

    CompletableFuture<NodeData> readNode(int indexId, long position, int chunk, KeyValueSize keyValueSize) throws InterruptedException, IOException, StorageException;

    CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, boolean isRoot, KeyValueSize keyValueSize) throws IOException, ExecutionException, InterruptedException, StorageException;

    CompletableFuture<Integer> updateNode(int indexId, byte[] data, Pointer pointer, boolean root) throws IOException, InterruptedException, StorageException;

    void close() throws IOException, StorageException;

    CompletableFuture<Integer> removeNode(int indexId, Pointer pointer, KeyValueSize keyValueSize) throws InterruptedException;

    boolean exists(int indexId);

    boolean supportsPurge();

    void purgeIndex(int indexId) throws IOException, InterruptedException, ExecutionException;
}
