package org.elece.storage.index;

import org.elece.memory.KvSize;
import org.elece.memory.Pointer;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface IndexStorageManager {
    byte[] getEmptyNode(KvSize kvSize);

    CompletableFuture<Optional<NodeData>> getRoot(int indexId, KvSize kvSize) throws InterruptedException;

    CompletableFuture<NodeData> readNode(int indexId, Pointer pointer, KvSize kvSize) throws InterruptedException, IOException;

    CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, boolean isRoot, KvSize size) throws IOException, ExecutionException, InterruptedException;

    CompletableFuture<Integer> updateNode(int indexId, byte[] data, Pointer pointer, boolean root) throws IOException, InterruptedException;

    CompletableFuture<Integer> removeNode(int indexId, Pointer pointer, KvSize size) throws InterruptedException;
}
