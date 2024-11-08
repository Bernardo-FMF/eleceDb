package org.elece.storage.index;

import org.elece.exception.FileChannelException;
import org.elece.exception.InterruptedTaskException;
import org.elece.exception.StorageException;
import org.elece.memory.KeyValueSize;
import org.elece.memory.Pointer;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface IndexStorageManager {
    CompletableFuture<Optional<NodeData>> getRoot(int indexId, KeyValueSize keyValueSize) throws StorageException,
                                                                                                 FileChannelException;

    byte[] getEmptyNode(KeyValueSize keyValueSize);

    default CompletableFuture<NodeData> readNode(int indexId, Pointer pointer, KeyValueSize keyValueSize) throws
                                                                                                          StorageException,
                                                                                                          FileChannelException {
        return this.readNode(indexId, pointer.getPosition(), pointer.getChunk(), keyValueSize);
    }

    CompletableFuture<NodeData> readNode(int indexId, long position, int chunk, KeyValueSize keyValueSize) throws
                                                                                                           StorageException,
                                                                                                           FileChannelException;

    CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, boolean isRoot, KeyValueSize keyValueSize) throws
                                                                                                                  StorageException,
                                                                                                                  InterruptedTaskException,
                                                                                                                  FileChannelException;

    CompletableFuture<Integer> updateNode(int indexId, byte[] data, Pointer pointer, boolean root) throws
                                                                                                   StorageException,
                                                                                                   FileChannelException;

    void close() throws StorageException, InterruptedTaskException, FileChannelException;

    CompletableFuture<Integer> removeNode(int indexId, Pointer pointer, KeyValueSize keyValueSize) throws
                                                                                                   StorageException,
                                                                                                   FileChannelException;

    boolean exists(int indexId);

    boolean supportsPurge();

    void purgeIndex(int indexId) throws InterruptedTaskException, StorageException, FileChannelException;
}
