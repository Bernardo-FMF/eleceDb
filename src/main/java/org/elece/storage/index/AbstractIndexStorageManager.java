package org.elece.storage.index;

import org.elece.config.DbConfig;
import org.elece.exception.storage.StorageException;
import org.elece.memory.KeyValueSize;
import org.elece.memory.Pointer;
import org.elece.storage.file.FileHandlerPool;
import org.elece.storage.index.header.IndexHeaderManager;
import org.elece.storage.index.header.IndexHeaderManagerFactory;
import org.elece.utils.BTreeUtils;
import org.elece.utils.FileUtils;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.elece.memory.tree.node.AbstractTreeNode.TYPE_INTERNAL_NODE_BIT;
import static org.elece.memory.tree.node.AbstractTreeNode.TYPE_LEAF_NODE_BIT;

public abstract class AbstractIndexStorageManager implements IndexStorageManager {
    public static final String INDEX_FILE_NAME = "index";

    protected final Path path;
    protected final String customName;
    protected final IndexHeaderManager indexHeaderManager;
    protected final DbConfig dbConfig;
    protected final FileHandlerPool fileHandlerPool;

    public AbstractIndexStorageManager(String customName, IndexHeaderManagerFactory indexHeaderManagerFactory, DbConfig dbConfig, FileHandlerPool fileHandlerPool) throws IOException, StorageException {
        this.customName = customName;
        this.indexHeaderManager = indexHeaderManagerFactory.getInstance(this.getHeaderPath());
        this.dbConfig = dbConfig;
        this.fileHandlerPool = fileHandlerPool;
        this.path = Path.of(dbConfig.getBaseDbPath());
    }

    protected int getBinarySpace(KeyValueSize keyValueSize) {
        return BTreeUtils.calculateBPlusTreeSize(this.dbConfig.getBTreeDegree(), keyValueSize.keySize(), keyValueSize.valueSize());
    }

    protected int getIndexGrowthAllocationSize(KeyValueSize keyValueSize) {
        return dbConfig.getBTreeGrowthNodeAllocationCount() * this.getBinarySpace(keyValueSize);
    }

    protected Path getHeaderPath() {
        return Path.of(path.toString(), "header.bin");
    }


    protected AsynchronousFileChannel acquireFileChannel(int indexId, int chunk) throws InterruptedException {
        Path indexFilePath = getIndexFilePath(indexId, chunk);
        try {
            return this.fileHandlerPool.acquireFileHandler(indexFilePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void releaseFileChannel(int indexId, int chunk) {
        Path indexFilePath = getIndexFilePath(indexId, chunk);
        this.fileHandlerPool.releaseFileHandler(indexFilePath);
    }

    protected abstract Path getIndexFilePath(int indexId, int chunk);

    protected abstract Pointer getAllocatedSpaceForNewNode(int indexId, int chunk, KeyValueSize keyValueSize) throws IOException, ExecutionException, InterruptedException, StorageException;

    protected abstract IndexHeaderManager.Location getIndexBeginningInChunk(int indexId, int chunk) throws InterruptedException, StorageException;

    @Override
    public CompletableFuture<Optional<NodeData>> getRoot(int indexId, KeyValueSize keyValueSize) throws InterruptedException, StorageException {
        CompletableFuture<Optional<NodeData>> output = new CompletableFuture<>();

        Optional<IndexHeaderManager.Location> optionalRootOfIndex = indexHeaderManager.getRootOfIndex(indexId);
        if (optionalRootOfIndex.isEmpty()) {
            output.complete(Optional.empty());
            return output;
        }

        IndexHeaderManager.Location rootLocation = optionalRootOfIndex.get();
        IndexHeaderManager.Location beginningInChunk = getIndexBeginningInChunk(indexId, rootLocation.chunk());

        FileUtils.readBytes(acquireFileChannel(indexId, rootLocation.chunk()), beginningInChunk.offset() + rootLocation.offset(), this.getBinarySpace(keyValueSize)).whenComplete((bytes, throwable) -> {
            releaseFileChannel(indexId, rootLocation.chunk());
            if (throwable != null) {
                output.completeExceptionally(throwable);
                return;
            }

            if (bytes.length == 0 || bytes[0] == (byte) 0x00) {
                output.complete(Optional.empty());
                return;
            }

            output.complete(Optional.of(new NodeData(new Pointer(Pointer.TYPE_NODE, rootLocation.offset(), rootLocation.chunk()), bytes)));
        });

        return output;
    }

    @Override
    public byte[] getEmptyNode(KeyValueSize keyValueSize) {
        return new byte[this.getBinarySpace(keyValueSize)];
    }

    @Override
    public CompletableFuture<NodeData> readNode(int indexId, long position, int chunk, KeyValueSize keyValueSize) throws InterruptedException, IOException, StorageException {
        CompletableFuture<NodeData> output = new CompletableFuture<>();

        IndexHeaderManager.Location beginningInChunk = getIndexBeginningInChunk(indexId, chunk);

        long filePosition = beginningInChunk.offset() + position;

        AsynchronousFileChannel asynchronousFileChannel = acquireFileChannel(indexId, chunk);
        if (asynchronousFileChannel.size() == 0) {
            releaseFileChannel(indexId, chunk);
            throw new IOException("Nothing available to read.");
        }

        FileUtils.readBytes(asynchronousFileChannel, filePosition, this.getBinarySpace(keyValueSize)).whenComplete((bytes, throwable) -> {
            releaseFileChannel(indexId, chunk);
            if (throwable != null) {
                output.completeExceptionally(throwable);
                return;
            }
            output.complete(new NodeData(new Pointer(Pointer.TYPE_NODE, position, chunk), bytes));
        });

        return output;
    }

    @Override
    public CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, boolean isRoot, KeyValueSize keyValueSize) throws IOException, ExecutionException, InterruptedException, StorageException {
        CompletableFuture<NodeData> output = new CompletableFuture<>();
        Pointer pointer = this.getAllocatedSpaceForNewNode(indexId, 0, keyValueSize);
        int binarySpace = this.getBinarySpace(keyValueSize);
        if (data.length < binarySpace) {
            byte[] finalData = new byte[binarySpace];
            // TODO: data.length < binarySpace causes an error (?)
            System.arraycopy(data, 0, finalData, 0, binarySpace);
            data = finalData;
        }

        byte[] finalData1 = data;
        long offset = pointer.getPosition();

        IndexHeaderManager.Location indexBeginningInChunk = this.getIndexBeginningInChunk(indexId, pointer.getChunk());

        pointer.setPosition(offset - indexBeginningInChunk.offset());

        FileUtils.write(acquireFileChannel(indexId, pointer.getChunk()), offset, data).whenComplete((size, throwable) -> {
            releaseFileChannel(indexId, pointer.getChunk());

            if (throwable != null) {
                output.completeExceptionally(throwable);
                return;
            }

            if (isRoot) {
                try {
                    this.updateRoot(indexId, pointer);
                } catch (StorageException exception) {
                    output.completeExceptionally(exception);
                    return;
                }
            }

            output.complete(new NodeData(pointer, finalData1));
        });
        return output;
    }


    protected Optional<Integer> getPossibleAllocationLocation(byte[] bytes, KeyValueSize keyValueSize) {
        for (int bTreeGrowthCount = 0; bTreeGrowthCount < dbConfig.getBTreeGrowthNodeAllocationCount(); bTreeGrowthCount++) {
            int position = bTreeGrowthCount * getBinarySpace(keyValueSize);
            if ((bytes[position] & TYPE_LEAF_NODE_BIT) != TYPE_LEAF_NODE_BIT && (bytes[position] & TYPE_INTERNAL_NODE_BIT) != TYPE_INTERNAL_NODE_BIT) {
                return Optional.of(position);
            }
        }
        return Optional.empty();
    }

    @Override
    public CompletableFuture<Integer> updateNode(int indexId, byte[] data, Pointer pointer, boolean isRoot) throws InterruptedException, StorageException {
        CompletableFuture<Integer> output = new CompletableFuture<>();

        long offset = getIndexBeginningInChunk(indexId, pointer.getChunk()).offset() + pointer.getPosition();

        AsynchronousFileChannel asynchronousFileChannel = acquireFileChannel(indexId, pointer.getChunk());

        FileUtils.write(asynchronousFileChannel, offset, data).whenComplete((integer, throwable) -> {
            releaseFileChannel(indexId, pointer.getChunk());
            if (isRoot) {
                try {
                    this.updateRoot(indexId, pointer);
                } catch (StorageException exception) {
                    output.completeExceptionally(exception);
                }
            }

            output.complete(integer);
        });

        return output;
    }

    private void updateRoot(int indexId, Pointer pointer) throws StorageException {
        indexHeaderManager.setRootOfIndex(indexId, IndexHeaderManager.Location.fromPointer(pointer));
    }

    @Override
    public void close() throws StorageException {
        this.fileHandlerPool.closeAll();
    }

    @Override
    public CompletableFuture<Integer> removeNode(int indexId, Pointer pointer, KeyValueSize keyValueSize) throws InterruptedException {
        Optional<IndexHeaderManager.Location> indexBeginningInChunk = indexHeaderManager.getIndexBeginningInChunk(indexId, pointer.getChunk());
        assert indexBeginningInChunk.isPresent();

        long offset = indexBeginningInChunk.get().offset() + pointer.getPosition();
        return FileUtils.write(acquireFileChannel(indexId, pointer.getChunk()), offset, new byte[this.getBinarySpace(keyValueSize)]).whenComplete((integer, throwable) -> releaseFileChannel(indexId, pointer.getChunk()));
    }

    @Override
    public boolean exists(int indexId) {
        Optional<IndexHeaderManager.Location> rootOfIndex = indexHeaderManager.getRootOfIndex(indexId);
        return rootOfIndex.filter(location -> Files.exists(getIndexFilePath(indexId, location.chunk()))).isPresent();
    }
}
