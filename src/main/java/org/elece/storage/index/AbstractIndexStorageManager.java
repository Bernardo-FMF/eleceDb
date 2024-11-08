package org.elece.storage.index;

import org.elece.config.DbConfig;
import org.elece.exception.*;
import org.elece.memory.KeyValueSize;
import org.elece.memory.Pointer;
import org.elece.storage.file.FileChannel;
import org.elece.storage.file.FileHandlerPool;
import org.elece.storage.index.header.IndexHeaderManager;
import org.elece.storage.index.header.IndexHeaderManagerFactory;
import org.elece.utils.BTreeUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.elece.memory.tree.node.AbstractTreeNode.TYPE_INTERNAL_NODE_BIT;
import static org.elece.memory.tree.node.AbstractTreeNode.TYPE_LEAF_NODE_BIT;

public abstract class AbstractIndexStorageManager implements IndexStorageManager {
    public static final String INDEX_FILE_NAME = "index";

    protected final Path path;
    protected final String customName;
    protected final IndexHeaderManager indexHeaderManager;
    protected final DbConfig dbConfig;
    protected final FileHandlerPool fileHandlerPool;

    protected AbstractIndexStorageManager(String customName, IndexHeaderManagerFactory indexHeaderManagerFactory,
                                          DbConfig dbConfig, FileHandlerPool fileHandlerPool) throws StorageException {
        this.customName = customName;
        this.path = Path.of(dbConfig.getBaseDbPath());
        this.indexHeaderManager = indexHeaderManagerFactory.getInstance(this.getHeaderPath());
        this.dbConfig = dbConfig;
        this.fileHandlerPool = fileHandlerPool;
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


    protected FileChannel acquireFileChannel(int indexId, int chunk) {
        try {
            Path indexFilePath = getIndexFilePath(indexId, chunk);
            return this.fileHandlerPool.acquireFileHandler(indexFilePath);
        } catch (StorageException | InterruptedTaskException exception) {
            throw new RuntimeDbException(exception.getDbError(), exception.getMessage());
        }
    }

    protected void releaseFileChannel(int indexId, int chunk) {
        try {
            Path indexFilePath = getIndexFilePath(indexId, chunk);
            this.fileHandlerPool.releaseFileHandler(indexFilePath);
        } catch (StorageException | FileChannelException | InterruptedTaskException exception) {
            throw new RuntimeDbException(exception.getDbError(), exception.getMessage());
        }
    }

    protected abstract Path getIndexFilePath(int indexId, int chunk);

    protected abstract Pointer getAllocatedSpaceForNewNode(int indexId, int chunk, KeyValueSize keyValueSize) throws
                                                                                                              StorageException,
                                                                                                              InterruptedTaskException,
                                                                                                              FileChannelException;

    protected abstract IndexHeaderManager.Location getIndexBeginningInChunk(int indexId, int chunk) throws
                                                                                                    StorageException;

    @Override
    public CompletableFuture<Optional<NodeData>> getRoot(int indexId, KeyValueSize keyValueSize) throws
                                                                                                 StorageException,
                                                                                                 FileChannelException {
        CompletableFuture<Optional<NodeData>> output = new CompletableFuture<>();

        Optional<IndexHeaderManager.Location> optionalRootOfIndex = indexHeaderManager.getRootOfIndex(indexId);
        if (optionalRootOfIndex.isEmpty()) {
            output.complete(Optional.empty());
            return output;
        }

        IndexHeaderManager.Location rootLocation = optionalRootOfIndex.get();
        IndexHeaderManager.Location beginningInChunk = getIndexBeginningInChunk(indexId, rootLocation.chunk());

        FileChannel fileChannel = acquireFileChannel(indexId, rootLocation.chunk());
        fileChannel.readAsync(beginningInChunk.offset() + rootLocation.offset(), this.getBinarySpace(keyValueSize))
                .whenComplete((bytes, throwable) -> {
                    releaseFileChannel(indexId, rootLocation.chunk());
                    if (throwable != null) {
                        output.completeExceptionally(new StorageException(DbError.FILE_READ_ERROR, "Failed to read from index file"));
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
    public CompletableFuture<NodeData> readNode(int indexId, long position, int chunk, KeyValueSize keyValueSize) throws
                                                                                                                  StorageException,
                                                                                                                  FileChannelException {
        CompletableFuture<NodeData> output = new CompletableFuture<>();

        IndexHeaderManager.Location beginningInChunk = getIndexBeginningInChunk(indexId, chunk);

        long filePosition = beginningInChunk.offset() + position;

        FileChannel fileChannel = acquireFileChannel(indexId, chunk);
        if (fileChannel.size() == 0) {
            releaseFileChannel(indexId, chunk);
            throw new StorageException(DbError.EMPTY_HEADER_FILE, "Empty header file");
        }

        fileChannel.readAsync(filePosition, this.getBinarySpace(keyValueSize))
                .whenComplete((bytes, throwable) -> {
                    releaseFileChannel(indexId, chunk);
                    if (throwable != null) {
                        output.completeExceptionally(new StorageException(DbError.FILE_READ_ERROR, "Failed to read from index file"));
                        return;
                    }
                    output.complete(new NodeData(new Pointer(Pointer.TYPE_NODE, position, chunk), bytes));
                });

        return output;
    }

    @Override
    public CompletableFuture<NodeData> writeNewNode(int indexId, byte[] data, boolean isRoot,
                                                    KeyValueSize keyValueSize) throws StorageException,
                                                                                      FileChannelException,
                                                                                      InterruptedTaskException {
        CompletableFuture<NodeData> output = new CompletableFuture<>();
        Pointer pointer = this.getAllocatedSpaceForNewNode(indexId, 0, keyValueSize);
        int binarySpace = this.getBinarySpace(keyValueSize);
        if (data.length < binarySpace) {
            byte[] finalData = new byte[binarySpace];
            System.arraycopy(data, 0, finalData, 0, data.length);
            data = finalData;
        }

        byte[] finalData1 = data;
        long offset = pointer.getPosition();

        IndexHeaderManager.Location indexBeginningInChunk = this.getIndexBeginningInChunk(indexId, pointer.getChunk());

        pointer.setPosition(offset - indexBeginningInChunk.offset());

        FileChannel fileChannel = acquireFileChannel(indexId, pointer.getChunk());
        fileChannel.writeAsync(offset, data).whenComplete((_, throwable) -> {
            releaseFileChannel(indexId, pointer.getChunk());

            if (throwable != null) {
                output.completeExceptionally(new StorageException(DbError.FILE_WRITE_ERROR, "Failed to write to index file"));
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
    public CompletableFuture<Integer> updateNode(int indexId, byte[] data, Pointer pointer, boolean isRoot) throws
                                                                                                            StorageException,
                                                                                                            FileChannelException {
        CompletableFuture<Integer> output = new CompletableFuture<>();

        long offset = getIndexBeginningInChunk(indexId, pointer.getChunk()).offset() + pointer.getPosition();

        FileChannel fileChannel = acquireFileChannel(indexId, pointer.getChunk());

        fileChannel.writeAsync(offset, data).whenComplete((updatedBytes, _) -> {
            releaseFileChannel(indexId, pointer.getChunk());
            if (isRoot) {
                try {
                    this.updateRoot(indexId, pointer);
                } catch (StorageException exception) {
                    output.completeExceptionally(exception);
                }
            }

            output.complete(updatedBytes);
        });

        return output;
    }

    private void updateRoot(int indexId, Pointer pointer) throws StorageException {
        indexHeaderManager.setRootOfIndex(indexId, IndexHeaderManager.Location.fromPointer(pointer));
    }

    @Override
    public void close() throws StorageException, InterruptedTaskException, FileChannelException {
        this.fileHandlerPool.closeAll();
    }

    @Override
    public CompletableFuture<Integer> removeNode(int indexId, Pointer pointer, KeyValueSize keyValueSize) throws
                                                                                                          StorageException,
                                                                                                          FileChannelException {
        Optional<IndexHeaderManager.Location> indexBeginningInChunk = indexHeaderManager.getIndexBeginningInChunk(indexId, pointer.getChunk());
        assert indexBeginningInChunk.isPresent();

        long offset = indexBeginningInChunk.get().offset() + pointer.getPosition();
        FileChannel fileChannel = acquireFileChannel(indexId, pointer.getChunk());
        return fileChannel.writeAsync(offset, new byte[this.getBinarySpace(keyValueSize)])
                .whenComplete((_, _) -> releaseFileChannel(indexId, pointer.getChunk()));
    }

    @Override
    public boolean exists(int indexId) {
        Optional<IndexHeaderManager.Location> rootOfIndex = indexHeaderManager.getRootOfIndex(indexId);
        return rootOfIndex.filter(location -> Files.exists(getIndexFilePath(indexId, location.chunk()))).isPresent();
    }
}
