package org.elece.storage.index;

import org.elece.config.DbConfig;
import org.elece.exception.DbError;
import org.elece.exception.FileChannelException;
import org.elece.exception.InterruptedTaskException;
import org.elece.exception.StorageException;
import org.elece.memory.KeyValueSize;
import org.elece.memory.Pointer;
import org.elece.storage.file.FileChannel;
import org.elece.storage.file.FileHandlerPool;
import org.elece.storage.index.header.IndexHeaderManager;
import org.elece.storage.index.header.IndexHeaderManagerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public class OrganizedIndexStorageManager extends AbstractIndexStorageManager {
    public OrganizedIndexStorageManager(String customName, IndexHeaderManagerFactory indexHeaderManagerFactory,
                                        DbConfig dbConfig, FileHandlerPool fileHandlerPool) throws IOException,
                                                                                                   StorageException {
        super(customName, indexHeaderManagerFactory, dbConfig, fileHandlerPool);
    }

    @Override
    protected Path getIndexFilePath(int indexId, int chunk) {
        return Path.of(path.toString(), String.format("%s.%s.%d", INDEX_FILE_NAME, customName, chunk));
    }

    @Override
    protected IndexHeaderManager.Location getIndexBeginningInChunk(int indexId, int chunk) throws StorageException {
        Optional<IndexHeaderManager.Location> optional = this.indexHeaderManager.getIndexBeginningInChunk(indexId, chunk);
        if (optional.isPresent()) {
            return optional.get();
        }

        List<Integer> indexesInChunk = this.indexHeaderManager.getIndexesInChunk(chunk);
        IndexHeaderManager.Location location;
        if (indexesInChunk.isEmpty()) {
            location = new IndexHeaderManager.Location(chunk, 0);
        } else {
            try {
                FileChannel fileChannel = this.acquireFileChannel(indexId, chunk);
                location = new IndexHeaderManager.Location(chunk, fileChannel.size());
            } catch (FileChannelException exception) {
                throw new StorageException(exception.getDbError(), exception.getMessage());
            } finally {
                this.releaseFileChannel(indexId, chunk);
            }
        }

        this.indexHeaderManager.setIndexBeginningInChunk(indexId, location);
        return location;
    }


    @Override
    protected Pointer getAllocatedSpaceForNewNode(int indexId, int chunk, KeyValueSize keyValueSize) throws
                                                                                                     StorageException,
                                                                                                     InterruptedTaskException,
                                                                                                     FileChannelException {
        Optional<IndexHeaderManager.Location> optionalIndexBeginningLocation = this.indexHeaderManager.getIndexBeginningInChunk(indexId, chunk);

        FileChannel fileChannel = this.acquireFileChannel(indexId, chunk);
        Long channelSize = fileChannel.size();

        // If we have a maximum file size, and that size is surpassed, we need to move to the next chunk and create a new file.
        if (this.dbConfig.getBTreeMaxFileSize() != -1 && channelSize >= this.dbConfig.getBTreeMaxFileSize()) {
            this.releaseFileChannel(indexId, chunk);
            return this.getAllocatedSpaceForNewNode(indexId, chunk + 1, keyValueSize);
        }

        // If it's a new chunk for this index, allocate at the end of the file!
        if (optionalIndexBeginningLocation.isEmpty()) {
            this.indexHeaderManager.setIndexBeginningInChunk(indexId, new IndexHeaderManager.Location(chunk, channelSize));
            Long position = fileChannel.allocate(this.getIndexGrowthAllocationSize(keyValueSize));
            this.releaseFileChannel(indexId, chunk);
            return new Pointer(Pointer.TYPE_NODE, position, chunk);
        }


        // If it's not a new chunk for this index, see if other indexes exist or not
        Optional<IndexHeaderManager.Location> nextIndexBeginningInChunk = this.indexHeaderManager.getNextIndexBeginningInChunk(indexId, chunk);
        long positionToCheck;
        // Another index exists, so lets check if we have a space left before the next index
        // Another index doesn't exist, so lets check if we have a space left in the end of the file
        positionToCheck = nextIndexBeginningInChunk
                .map(location -> location.offset() - this.getIndexGrowthAllocationSize(keyValueSize))
                .orElseGet(() -> channelSize - this.getIndexGrowthAllocationSize(keyValueSize));

        if (positionToCheck >= 0) {
            // Check if we have an empty space
            byte[] bytes = fileChannel.read(positionToCheck, this.getIndexGrowthAllocationSize(keyValueSize));
            Optional<Integer> optionalAdditionalPosition = getPossibleAllocationLocation(bytes, keyValueSize);
            if (optionalAdditionalPosition.isPresent()) {
                long finalPosition = positionToCheck + optionalAdditionalPosition.get();
                this.releaseFileChannel(indexId, chunk);
                return new Pointer(Pointer.TYPE_NODE, finalPosition, chunk);
            }
        }

        // Empty space not found, allocate in the end or before next index
        long allocatedOffset;
        if (nextIndexBeginningInChunk.isPresent()) {
            allocatedOffset = fileChannel.allocate(nextIndexBeginningInChunk.get().offset(), this.getIndexGrowthAllocationSize(keyValueSize));
            Optional<Integer> nextIndex = this.indexHeaderManager.getNextIndexIdInChunk(indexId, chunk);
            if (nextIndex.isEmpty()) {
                throw new StorageException(DbError.INDEX_NOT_FOUND_IN_CHUNK_ERROR, "Failed to find next index in chunk");
            }
            this.indexHeaderManager.setIndexBeginningInChunk(nextIndex.get(), new IndexHeaderManager.Location(chunk, nextIndexBeginningInChunk.get().offset() + this.getIndexGrowthAllocationSize(keyValueSize)));
        } else {
            allocatedOffset = fileChannel.allocate(this.getIndexGrowthAllocationSize(keyValueSize));
        }

        this.releaseFileChannel(indexId, chunk);

        return new Pointer(Pointer.TYPE_NODE, allocatedOffset, chunk);
    }

    @Override
    public boolean supportsPurge() {
        return true;
    }

    @Override
    public void purgeIndex(int indexId) throws StorageException, InterruptedTaskException, FileChannelException {
        List<Integer> chunksOfIndex = this.indexHeaderManager.getChunksOfIndex(indexId);
        for (Integer chunk : chunksOfIndex) {
            Optional<IndexHeaderManager.Location> optionalLocation = this.indexHeaderManager.getIndexBeginningInChunk(indexId, chunk);
            if (optionalLocation.isEmpty()) {
                continue;
            }

            Path indexFilePath = getIndexFilePath(indexId, 0);
            FileChannel fileChannel = this.fileHandlerPool.acquireFileHandler(indexFilePath);

            IndexHeaderManager.Location location = optionalLocation.get();
            Optional<IndexHeaderManager.Location> nextIndexBeginningInChunk = this.indexHeaderManager.getNextIndexBeginningInChunk(indexId, chunk);
            long size;
            if (nextIndexBeginningInChunk.isPresent()) {
                size = nextIndexBeginningInChunk.get().offset() - location.offset();
            } else {
                size = fileChannel.size() - location.offset();
            }
            byte[] bytes = new byte[Math.toIntExact(size)];
            fileChannel.write(location.offset(), bytes);

            fileHandlerPool.releaseFileHandler(indexFilePath);
        }
    }
}
