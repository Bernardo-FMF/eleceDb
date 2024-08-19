package org.elece.storage.index;

import org.elece.config.IDbConfig;
import org.elece.memory.KvSize;
import org.elece.memory.Pointer;
import org.elece.storage.file.IFileHandlerPool;
import org.elece.storage.index.header.IndexHeaderManager;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public abstract class AbstractIndexStorageManager implements IndexStorageManager {
    public static final String INDEX_FILE_NAME = "index";

    protected final Path path;
    protected final String customName;
    protected final IndexHeaderManager indexHeaderManager;
    protected final IDbConfig dbConfig;
    protected final IFileHandlerPool fileHandlerPool;

    public AbstractIndexStorageManager(Path path, IndexHeaderManager indexHeaderManager, IDbConfig dbConfig, IFileHandlerPool fileHandlerPool) {
        this(path, null, indexHeaderManager, dbConfig, fileHandlerPool);
    }

    public AbstractIndexStorageManager(Path path, String customName, IndexHeaderManager indexHeaderManager, IDbConfig dbConfig, IFileHandlerPool fileHandlerPool) {
        this.path = path;
        this.customName = customName;
        this.indexHeaderManager = indexHeaderManager;
        this.dbConfig = dbConfig;
        this.fileHandlerPool = fileHandlerPool;
    }

    @Override
    public CompletableFuture<Optional<NodeData>> getRoot(int indexId, KvSize KvSize) throws InterruptedException {
        return null;
    }

    @Override
    public byte[] getEmptyNode(KvSize KvSize) {
        return new byte[0];
    }

    @Override
    public CompletableFuture<NodeData> readNode(int indexId, long position, int chunk, KvSize KvSize) throws InterruptedException, IOException {
        return null;
    }

    @Override
    public CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, boolean isRoot, KvSize size) throws IOException, ExecutionException, InterruptedException {
        return null;
    }

    @Override
    public CompletableFuture<Integer> updateNode(int indexId, byte[] data, Pointer pointer, boolean root) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public CompletableFuture<Integer> removeNode(int indexId, Pointer pointer, KvSize size) throws InterruptedException {
        return null;
    }

    @Override
    public boolean exists(int indexId) {
        return false;
    }
}
