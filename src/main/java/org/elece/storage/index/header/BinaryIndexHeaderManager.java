package org.elece.storage.index.header;

import org.elece.memory.BinaryUtils;
import org.elece.storage.error.StorageException;
import org.elece.storage.error.type.InternalStorageError;
import org.elece.storage.file.IFileHandlerPool;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * The file structure should consist of 4 bytes, corresponding to an integer that reflects the number of roots that need to be read from the file.
 * Then the roots are followed, represented as such:
 * | 4 bytes (key) | 4 bytes + 8 bytes (location) |
 * Then, for the chunks of the locations, we also store a list of index offsets relative to the current chunk, represented like this:
 * | 4 bytes (chunk) | 4 bytes (size of the chunk offset list) | 4 bytes + 8 bytes (each element of the index offset list) |
 */
public class BinaryIndexHeaderManager implements IndexHeaderManager {
    private final IFileHandlerPool fileHandlerPool;
    private final Path path;
    private final IndexHeader header;

    public BinaryIndexHeaderManager(Path path, IFileHandlerPool fileHandlerPool) throws IOException, StorageException {
        this.fileHandlerPool = fileHandlerPool;
        this.path = path;
        File file = this.path.toFile();

        if (!file.exists()) {
            boolean createdFile = file.createNewFile();
            if (!createdFile) {
                throw new StorageException(new InternalStorageError("Couldn't create index header file(s)"));
            } else {
                this.header = new IndexHeader();
            }
            return;
        }

        this.header = readIndexHeader();
    }

    private void writeIndexHeader() throws StorageException {
        try (AsynchronousFileChannel asynchronousFileChannel = fileHandlerPool.acquireFileHandler(path)) {
            long position = 0;

            Map<Integer, Location> roots = header.getRoots();

            ByteBuffer mapSize = ByteBuffer.allocate(roots.size() + (roots.size() * (Integer.BYTES + Long.BYTES)));
            mapSize.putInt(roots.size());

            asynchronousFileChannel.write(mapSize, position).get();

            position += mapSize.capacity();

            ByteBuffer rootBuffer = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + Long.BYTES);

            for (Map.Entry<Integer, Location> entry : roots.entrySet()) {
                Integer indexId = entry.getKey();
                Location location = entry.getValue();

                rootBuffer.putInt(indexId);
                rootBuffer.putInt(location.chunk());
                rootBuffer.putLong(location.offset());

                asynchronousFileChannel.write(rootBuffer, position).get();

                position += rootBuffer.capacity();

                rootBuffer.clear();
            }

            ByteBuffer offsetBuffer = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES);
            ByteBuffer offsetListElementBuffer = ByteBuffer.allocate(Integer.BYTES + Long.BYTES);

            for (Map.Entry<Integer, TreeSet<IndexHeader.IndexOffset>> entry : header.getChunkIndexOffsets().entrySet()) {
                offsetBuffer.putInt(entry.getKey());
                offsetBuffer.putInt(entry.getValue().size());

                asynchronousFileChannel.write(offsetBuffer, position).get();

                position += offsetBuffer.capacity();

                offsetBuffer.clear();

                for (IndexHeader.IndexOffset indexOffset : entry.getValue()) {
                    offsetListElementBuffer.putInt(indexOffset.getIndexId());
                    offsetListElementBuffer.putLong(indexOffset.getOffset());

                    asynchronousFileChannel.write(offsetListElementBuffer, position).get();

                    position += offsetListElementBuffer.capacity();

                    offsetListElementBuffer.clear();
                }
            }
        } catch (IOException | InterruptedException | ExecutionException exception) {
            throw new StorageException(new InternalStorageError("Couldn't write index header file(s)"));
        } finally {
            fileHandlerPool.releaseFileHandler(path);
        }
    }

    private IndexHeader readIndexHeader() throws StorageException {
        try (AsynchronousFileChannel asynchronousFileChannel = fileHandlerPool.acquireFileHandler(path)) {
            long position = 0;
            ByteBuffer mapSize = ByteBuffer.allocate(Integer.BYTES);

            asynchronousFileChannel.read(mapSize, position);
            position += mapSize.capacity();

            int numOfRoots = BinaryUtils.bytesToInteger(mapSize.array(), 0);

            ByteBuffer rootBuffer = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + Long.BYTES);

            Map<Integer, Location> roots = new ConcurrentHashMap<>();

            for (int rootIndex = 0; rootIndex < numOfRoots; rootIndex++) {
                asynchronousFileChannel.read(rootBuffer, position);
                byte[] rootBytes = rootBuffer.array();

                int indexId = BinaryUtils.bytesToInteger(rootBytes, 0);
                int locationChunk = BinaryUtils.bytesToInteger(rootBytes, Integer.BYTES);
                long locationOffset = BinaryUtils.bytesToLong(rootBytes, Integer.BYTES * 2);

                roots.put(indexId, new Location(locationChunk, locationOffset));

                position += rootBuffer.capacity();

                rootBuffer.clear();
            }

            Map<Integer, TreeSet<IndexHeader.IndexOffset>> chunkIndexOffsets = new ConcurrentHashMap<>();

            ByteBuffer chunkInfoBuffer = ByteBuffer.allocate(Long.BYTES);
            ByteBuffer indexOffsetBuffer = ByteBuffer.allocate(Integer.BYTES + Long.BYTES);

            for (long chunkIndex = 0; chunkIndex < numOfRoots; chunkIndex++) {
                asynchronousFileChannel.read(chunkInfoBuffer, position);
                byte[] chunkInfoBytes = chunkInfoBuffer.array();

                int chunk = BinaryUtils.bytesToInteger(chunkInfoBytes, 0);
                int chunkOffsetsSize = BinaryUtils.bytesToInteger(chunkInfoBytes, Integer.BYTES);

                TreeSet<IndexHeader.IndexOffset> offsetSet = new TreeSet<>(Comparator.comparingLong(IndexHeader.IndexOffset::getOffset));

                position += chunkInfoBuffer.capacity();

                for (int offsetIndex = 0; offsetIndex < chunkOffsetsSize; offsetIndex++) {
                    asynchronousFileChannel.read(indexOffsetBuffer, position);
                    byte[] offsetBytes = indexOffsetBuffer.array();

                    int indexId = BinaryUtils.bytesToInteger(offsetBytes, 0);
                    long offset = BinaryUtils.bytesToLong(offsetBytes, Integer.BYTES);

                    offsetSet.add(new IndexHeader.IndexOffset(indexId, offset));

                    position += indexOffsetBuffer.capacity();

                    indexOffsetBuffer.clear();
                }

                chunkIndexOffsets.put(chunk, offsetSet);

                chunkInfoBuffer.clear();
            }

            return new IndexHeader(roots, chunkIndexOffsets);
        } catch (IOException | InterruptedException exception) {
            throw new StorageException(new InternalStorageError("Couldn't read index header file(s)"));
        } finally {
            fileHandlerPool.releaseFileHandler(path);
        }
    }

    @Override
    public Optional<Location> getRootOfIndex(int indexId) {
        return Optional.ofNullable(header.getRoots().get(indexId));
    }

    @Override
    public void setRootOfIndex(int indexId, Location location) throws StorageException {
        header.addRoot(indexId, location);
        writeIndexHeader();
    }

    @Override
    public void setIndexBeginningInChunk(int indexId, Location location) throws StorageException {
        TreeSet<IndexHeader.IndexOffset> indexOffsets = this.header.getIndexOffsets(location.chunk());

        synchronized (this) {
            Optional<IndexHeader.IndexOffset> optionalIndexOffset = indexOffsets.stream().filter(indexOffset -> indexOffset.getIndexId() == indexId).findFirst();
            if (optionalIndexOffset.isPresent()) {
                optionalIndexOffset.get().setOffset(location.offset());
            } else {
                indexOffsets.add(new IndexHeader.IndexOffset(indexId, location.offset()));
            }

            writeIndexHeader();
        }
    }

    @Override
    public List<Integer> getIndexesInChunk(int chunk) {
        TreeSet<IndexHeader.IndexOffset> indexOffsets = this.header.getIndexOffsets(chunk);
        List<Integer> indexes = new ArrayList<>(indexOffsets.size());
        for (IndexHeader.IndexOffset indexOffset : indexOffsets) {
            indexes.add(indexOffset.getIndexId());
        }
        indexes.sort(Comparator.naturalOrder());
        return indexes;
    }

    @Override
    public Optional<Integer> getNextIndexIdInChunk(int indexId, int chunk) {
        TreeSet<IndexHeader.IndexOffset> indexOffsets = this.header.getIndexOffsets(chunk);

        Optional<IndexHeader.IndexOffset> current = indexOffsets.stream()
                .filter(offset -> offset.getIndexId() == indexId)
                .findFirst();

        if (current.isEmpty()) {
            return Optional.empty();
        }

        IndexHeader.IndexOffset next = indexOffsets.higher(current.get());

        return next != null ? Optional.of(next.getIndexId()) : Optional.empty();
    }

    @Override
    public List<Integer> getChunksOfIndex(int indexId) {
        List<Integer> chunks = new ArrayList<>();
        Map<Integer, TreeSet<IndexHeader.IndexOffset>> chunkIndexOffset = this.header.getChunkIndexOffsets();
        chunkIndexOffset.forEach((chunk, indexOffsets) -> {
            indexOffsets.stream().filter(indexOffset -> indexOffset.getIndexId() == indexId).findAny().ifPresent(indexOffset -> chunks.add(chunk));
        });
        return chunks;
    }
}
