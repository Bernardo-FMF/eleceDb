package org.elece.storage.index;

import org.elece.config.DbConfig;
import org.elece.exception.StorageException;
import org.elece.memory.KeyValueSize;
import org.elece.memory.Pointer;
import org.elece.storage.file.FileHandlerPool;
import org.elece.storage.index.header.IndexHeaderManager;
import org.elece.storage.index.header.IndexHeaderManagerFactory;
import org.elece.utils.FileUtils;

import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.Optional;

public class CompactIndexStorageManager extends AbstractIndexStorageManager {
    public CompactIndexStorageManager(String customName, IndexHeaderManagerFactory indexHeaderManagerFactory,
                                      DbConfig dbConfig, FileHandlerPool fileHandlerPool) throws StorageException {
        super(customName, indexHeaderManagerFactory, dbConfig, fileHandlerPool);
    }

    @Override
    protected Path getIndexFilePath(int indexId, int chunk) {
        return Path.of(path.toString(), String.format("%s.bin", INDEX_FILE_NAME));
    }

    @Override
    protected Pointer getAllocatedSpaceForNewNode(int indexId, int chunk, KeyValueSize keyValueSize) throws
                                                                                                     StorageException {
        Path indexFilePath = getIndexFilePath(indexId, 0);
        AsynchronousFileChannel asynchronousFileChannel = this.fileHandlerPool.acquireFileHandler(indexFilePath);

        synchronized (this) {
            long fileSize = FileUtils.getFileSize(asynchronousFileChannel);
            if (fileSize >= this.getIndexGrowthAllocationSize(keyValueSize)) {
                long positionToCheck = fileSize - this.getIndexGrowthAllocationSize(keyValueSize);

                byte[] bytes = FileUtils.readBytes(asynchronousFileChannel, positionToCheck, this.getIndexGrowthAllocationSize(keyValueSize));
                Optional<Integer> optionalAdditionalPosition = getPossibleAllocationLocation(bytes, keyValueSize);
                if (optionalAdditionalPosition.isPresent()) {
                    long finalPosition = positionToCheck + optionalAdditionalPosition.get();
                    fileHandlerPool.releaseFileHandler(indexFilePath);
                    return new Pointer(Pointer.TYPE_NODE, finalPosition, chunk);
                }
            }

            Long position = FileUtils.allocate(asynchronousFileChannel, this.getIndexGrowthAllocationSize(keyValueSize));
            fileHandlerPool.releaseFileHandler(indexFilePath);
            return new Pointer(Pointer.TYPE_NODE, position, chunk);
        }
    }

    @Override
    protected IndexHeaderManager.Location getIndexBeginningInChunk(int indexId, int chunk) {
        return new IndexHeaderManager.Location(0, 0);
    }

    @Override
    public boolean supportsPurge() {
        return false;
    }

    @Override
    public void purgeIndex(int indexId) {
        // Purge is not supported
    }
}
