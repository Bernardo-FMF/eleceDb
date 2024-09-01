package org.elece.storage.index.header;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import org.elece.exception.storage.StorageException;
import org.elece.exception.storage.type.InternalStorageError;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

public class JsonIndexHeaderManager implements IndexHeaderManager {
    private final Path path;
    private IndexHeader header;
    private final Gson gson;

    public JsonIndexHeaderManager(Path path) throws IOException, StorageException {
        this.path = path;
        this.gson = new GsonBuilder().serializeNulls().create();
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

        loadIndexHeader(file);
    }

    private void persistIndexHeader() throws StorageException {
        try (FileWriter writer = new FileWriter(this.path.toFile())) {
            gson.toJson(header, writer);
        } catch (IOException e) {
            throw new StorageException(new InternalStorageError("Couldn't write index header file(s)"));
        }
    }

    private void loadIndexHeader(File file) throws StorageException {
        JsonReader jsonReader;
        try {
            jsonReader = new JsonReader(new FileReader(file));
            IndexHeader newIndexHeader = gson.fromJson(jsonReader, IndexHeader.class);
            if (Objects.isNull(newIndexHeader)) {
                this.header = new IndexHeader();
                persistIndexHeader();
            } else {
                this.header = newIndexHeader;
            }
        } catch (FileNotFoundException e) {
            throw new StorageException(new InternalStorageError("Couldn't read index header file(s)"));
        }
    }

    @Override
    public Optional<Location> getRootOfIndex(int indexId) {
        return Optional.ofNullable(header.getRoots().get(indexId));
    }

    @Override
    public void setRootOfIndex(int indexId, Location location) throws StorageException {
        header.addRoot(indexId, location);
        persistIndexHeader();
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

            persistIndexHeader();
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

    @Override
    public Optional<Location> getIndexBeginningInChunk(int indexId, int chunk) {
        TreeSet<IndexHeader.IndexOffset> indexOffsets = this.header.getIndexOffsets(chunk);
        Optional<IndexHeader.IndexOffset> optionalIndexOffset = indexOffsets.stream().filter(indexOffset -> indexOffset.getIndexId() == indexId).findFirst();
        return optionalIndexOffset.map(indexOffset -> new Location(chunk, indexOffset.getOffset()));
    }

    @Override
    public Optional<Location> getNextIndexBeginningInChunk(int indexId, int chunk) {
        Optional<IndexHeader.IndexOffset> nextIndexOffset = this.header.getNextIndexOffset(chunk, indexId);
        return nextIndexOffset.map(indexOffset -> new Location(chunk, indexOffset.getOffset()));
    }
}
