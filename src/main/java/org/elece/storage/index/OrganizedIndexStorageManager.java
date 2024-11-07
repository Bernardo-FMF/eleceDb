package org.elece.storage.index;

import org.elece.config.DbConfig;
import org.elece.exception.StorageException;
import org.elece.memory.KeyValueSize;
import org.elece.memory.Pointer;
import org.elece.storage.file.FileHandlerPool;
import org.elece.storage.index.header.IndexHeaderManager;
import org.elece.storage.index.header.IndexHeaderManagerFactory;
import org.elece.utils.FileUtils;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class OrganizedIndexStorageManager extends AbstractIndexStorageManager {
    public OrganizedIndexStorageManager(String customName, IndexHeaderManagerFactory indexHeaderManagerFactory, DbConfig dbConfig, FileHandlerPool fileHandlerPool) throws IOException, StorageException {
        super(customName, indexHeaderManagerFactory, dbConfig, fileHandlerPool);
    }

    @Override
    protected Path getIndexFilePath(int indexId, int chunk) {
        return Path.of(path.toString(), String.format("%s.%s.%d", INDEX_FILE_NAME, customName, chunk));
    }

    @Override
    protected IndexHeaderManager.Location getIndexBeginningInChunk(int indexId, int chunk) throws InterruptedException, StorageException {
        Optional<IndexHeaderManager.Location> optional = this.indexHeaderManager.getIndexBeginningInChunk(indexId, chunk);
        if (optional.isPresent()) {
            return optional.get();
        }

        List<Integer> indexesInChunk = this.indexHeaderManager.getIndexesInChunk(chunk);
        IndexHeaderManager.Location location;
        if (indexesInChunk.isEmpty()) {
            location = new IndexHeaderManager.Location(chunk, 0);
        } else {
            try (AsynchronousFileChannel asynchronousFileChannel = this.acquireFileChannel(indexId, chunk)) {
                location = new IndexHeaderManager.Location(chunk, asynchronousFileChannel.size());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            this.releaseFileChannel(indexId, chunk);
        }

        this.indexHeaderManager.setIndexBeginningInChunk(indexId, location);
        return location;
    }


    @Override
    protected Pointer getAllocatedSpaceForNewNode(int indexId, int chunk, KeyValueSize keyValueSize) throws IOException, ExecutionException, InterruptedException, StorageException {
        Optional<IndexHeaderManager.Location> optionalIndexBeginningLocation = this.indexHeaderManager.getIndexBeginningInChunk(indexId, chunk);

        AsynchronousFileChannel asynchronousFileChannel = this.acquireFileChannel(indexId, chunk);

        // If we have a maximum file size, and that size is surpassed, we need to move to the next chunk and create a new file.
        if (this.dbConfig.getBTreeMaxFileSize() != -1 && asynchronousFileChannel.size() >= this.dbConfig.getBTreeMaxFileSize()) {
            this.releaseFileChannel(indexId, chunk);
            return this.getAllocatedSpaceForNewNode(indexId, chunk + 1, keyValueSize);
        }

        // If it's a new chunk for this index, allocate at the end of the file!
        if (optionalIndexBeginningLocation.isEmpty()) {
            this.indexHeaderManager.setIndexBeginningInChunk(indexId, new IndexHeaderManager.Location(chunk, asynchronousFileChannel.size()));
            Long position = FileUtils.allocate(asynchronousFileChannel, this.getIndexGrowthAllocationSize(keyValueSize)).get();
            this.releaseFileChannel(indexId, chunk);
            return new Pointer(Pointer.TYPE_NODE, position, chunk);
        }


        // If it's not a new chunk for this index, see if other indexes exist or not

        Optional<IndexHeaderManager.Location> nextIndexBeginningInChunk = this.indexHeaderManager.getNextIndexBeginningInChunk(indexId, chunk);
        long positionToCheck;
        if (nextIndexBeginningInChunk.isPresent()) {
            // Another index exists, so lets check if we have a space left before the next index
            positionToCheck = nextIndexBeginningInChunk.get().offset() - this.getIndexGrowthAllocationSize(keyValueSize);
        } else {
            // Another index doesn't exist, so lets check if we have a space left in the end of the file
            positionToCheck = asynchronousFileChannel.size() - this.getIndexGrowthAllocationSize(keyValueSize);
        }

        if (positionToCheck >= 0) {
            // Check if we have an empty space
            byte[] bytes = FileUtils.readBytes(asynchronousFileChannel, positionToCheck, this.getIndexGrowthAllocationSize(keyValueSize)).get();
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
            allocatedOffset = FileUtils.allocate(
                    asynchronousFileChannel,
                    nextIndexBeginningInChunk.get().offset(),
                    this.getIndexGrowthAllocationSize(keyValueSize)
            ).get();
            Integer nextIndex = this.indexHeaderManager.getNextIndexIdInChunk(indexId, chunk).get();
            this.indexHeaderManager.setIndexBeginningInChunk(nextIndex, new IndexHeaderManager.Location(chunk, nextIndexBeginningInChunk.get().offset() + this.getIndexGrowthAllocationSize(keyValueSize)));
        } else {
            allocatedOffset = FileUtils.allocate(
                    asynchronousFileChannel,
                    this.getIndexGrowthAllocationSize(keyValueSize)
            ).get();
        }

        this.releaseFileChannel(indexId, chunk);

        return new Pointer(Pointer.TYPE_NODE, allocatedOffset, chunk);
    }

    @Override
    public boolean supportsPurge() {
        return true;
    }

    @Override
    public void purgeIndex(int indexId) throws IOException, InterruptedException, ExecutionException {
        List<Integer> chunksOfIndex = this.indexHeaderManager.getChunksOfIndex(indexId);
        for (Integer chunk : chunksOfIndex) {
            Optional<IndexHeaderManager.Location> optionalLocation = this.indexHeaderManager.getIndexBeginningInChunk(indexId, chunk);
            if (optionalLocation.isEmpty()) {
                continue;
            }

            Path indexFilePath = getIndexFilePath(indexId, 0);
            AsynchronousFileChannel asynchronousFileChannel = this.fileHandlerPool.acquireFileHandler(indexFilePath);

            IndexHeaderManager.Location location = optionalLocation.get();
            Optional<IndexHeaderManager.Location> nextIndexBeginningInChunk = this.indexHeaderManager.getNextIndexBeginningInChunk(indexId, chunk);
            long size;
            if (nextIndexBeginningInChunk.isPresent()) {
                size = nextIndexBeginningInChunk.get().offset() - location.offset();
            } else {
                size = asynchronousFileChannel.size() - location.offset();
            }
            byte[] bytes = new byte[Math.toIntExact(size)];
            FileUtils.write(asynchronousFileChannel, location.offset(), bytes).get();

            fileHandlerPool.releaseFileHandler(indexFilePath);
        }
    }
}
