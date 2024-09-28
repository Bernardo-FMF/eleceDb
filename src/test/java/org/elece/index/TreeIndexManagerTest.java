package org.elece.index;

import org.elece.config.DbConfig;
import org.elece.config.DefaultDbConfigBuilder;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.storage.StorageException;
import org.elece.memory.Pointer;
import org.elece.memory.tree.node.DefaultNodeFactory;
import org.elece.memory.tree.node.data.IntegerBinaryObject;
import org.elece.memory.tree.node.data.PointerBinaryObject;
import org.elece.memory.tree.node.data.StringBinaryObject;
import org.elece.storage.file.DefaultFileHandlerFactory;
import org.elece.storage.file.UnrestrictedFileHandlerPool;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.OrganizedIndexStorageManager;
import org.elece.storage.index.header.DefaultIndexHeaderManagerFactory;
import org.elece.storage.index.session.factory.ImmediateIOSessionFactory;
import org.elece.utils.BTreeUtils;
import org.elece.utils.FileTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.elece.storage.index.AbstractIndexStorageManager.INDEX_FILE_NAME;

class TreeIndexManagerTest {
    public static final int MAX_STRING_SIZE = 255;
    private Path dbPath;
    private DbConfig integerDbConfig;
    private DbConfig stringDbConfig;

    private IndexStorageManager indexStorageManager;

    private IntegerBinaryObject.Factory integerKBinaryObjectFactory;
    private StringBinaryObject.Factory stringKBinaryObjectFactory;
    private PointerBinaryObject.Factory vBinaryObjectFactory;

    @BeforeEach
    public void setUp() throws IOException, StorageException {
        dbPath = Files.createTempDirectory("Tree_Index_Manager_Test_Case");

        integerDbConfig = DefaultDbConfigBuilder.builder()
                .setBaseDbPath(dbPath.toString())
                .setbTreeDegree(4)
                .setbTreeGrowthNodeAllocationCount(2)
                .setbTreeMaxFileSize(4L * BTreeUtils.calculateBPlusTreeSize(4, IntegerBinaryObject.BYTES, PointerBinaryObject.BYTES))
                .build();

        stringDbConfig = DefaultDbConfigBuilder.builder()
                .setBaseDbPath(dbPath.toString())
                .setbTreeDegree(4)
                .setbTreeGrowthNodeAllocationCount(2)
                .setbTreeMaxFileSize(4L * BTreeUtils.calculateBPlusTreeSize(4, MAX_STRING_SIZE, PointerBinaryObject.BYTES))
                .build();

        byte[] writingBytes = new byte[]{};
        Path indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        indexStorageManager = new OrganizedIndexStorageManager("test", new DefaultIndexHeaderManagerFactory(), integerDbConfig, new UnrestrictedFileHandlerPool(DefaultFileHandlerFactory.getInstance(), integerDbConfig));
        integerKBinaryObjectFactory = new IntegerBinaryObject.Factory();
        stringKBinaryObjectFactory = new StringBinaryObject.Factory(MAX_STRING_SIZE);
        vBinaryObjectFactory = new PointerBinaryObject.Factory();
    }

    @AfterEach
    public void destroy() throws IOException {
        FileTestUtils.deleteDirectory(dbPath.toString());
    }

    @Test
    public void test_tryToAddZero() {
        IndexManager<Integer, Pointer> indexManager = new TreeIndexManager<>(1, indexStorageManager, ImmediateIOSessionFactory.getInstance(), integerDbConfig,
                integerKBinaryObjectFactory, vBinaryObjectFactory, new DefaultNodeFactory<>(integerKBinaryObjectFactory, vBinaryObjectFactory));

        Assertions.assertThrows(BTreeException.class, () -> indexManager.addIndex(0, Pointer.empty()));
    }

    @Test
    public void test_addIntegerIndexes() throws BTreeException, StorageException {
        IndexManager<Integer, Pointer> indexManager = new TreeIndexManager<>(1, indexStorageManager, ImmediateIOSessionFactory.getInstance(), integerDbConfig,
                integerKBinaryObjectFactory, vBinaryObjectFactory, new DefaultNodeFactory<>(integerKBinaryObjectFactory, vBinaryObjectFactory));

        for (int index = 1; index <= 3; index++) {
            Pointer pointer = new Pointer(Pointer.TYPE_DATA, Pointer.BYTES * (index - 1) + index, 1);
            indexManager.addIndex(index, pointer);

            Optional<Pointer> retrievedPointer = indexManager.getIndex(index);
            Assertions.assertTrue(retrievedPointer.isPresent());
            Assertions.assertEquals(pointer, retrievedPointer.get());

            Assertions.assertTrue(indexManager.removeIndex(index));
            Assertions.assertTrue(indexManager.getIndex(index).isEmpty());
        }
    }


    @Test
    public void test_addStringIndexes() throws BTreeException, StorageException {
        IndexManager<String, Pointer> stringIndexManager = new TreeIndexManager<>(1, indexStorageManager, ImmediateIOSessionFactory.getInstance(), stringDbConfig,
                stringKBinaryObjectFactory, vBinaryObjectFactory, new DefaultNodeFactory<>(stringKBinaryObjectFactory, vBinaryObjectFactory));

        for (int index = 1; index <= 3; index++) {
            String stringIndex = "index" + index;
            Pointer pointer = new Pointer(Pointer.TYPE_DATA, Pointer.BYTES * (index - 1) + index, 1);
            stringIndexManager.addIndex(stringIndex, pointer);

            Optional<Pointer> retrievedPointer = stringIndexManager.getIndex(stringIndex);
            Assertions.assertTrue(retrievedPointer.isPresent());
            Assertions.assertEquals(pointer, retrievedPointer.get());

            Assertions.assertTrue(stringIndexManager.removeIndex(stringIndex));
            Assertions.assertTrue(stringIndexManager.getIndex(stringIndex).isEmpty());
        }
    }

    @Test
    public void test_updateIntegerIndexes() throws BTreeException, StorageException {
        IndexManager<Integer, Pointer> indexManager = new TreeIndexManager<>(1, indexStorageManager, ImmediateIOSessionFactory.getInstance(), integerDbConfig,
                integerKBinaryObjectFactory, vBinaryObjectFactory, new DefaultNodeFactory<>(integerKBinaryObjectFactory, vBinaryObjectFactory));

        for (int index = 1; index <= 3; index++) {
            Pointer pointer = new Pointer(Pointer.TYPE_DATA, Pointer.BYTES * (index - 1) + index, 1);
            indexManager.addIndex(index, pointer);

            Optional<Pointer> retrievedPointer = indexManager.getIndex(index);
            Assertions.assertTrue(retrievedPointer.isPresent());
            Assertions.assertEquals(pointer, retrievedPointer.get());

            Pointer updatedPointer = new Pointer(Pointer.TYPE_DATA, Pointer.BYTES * (index - 1) + index + 10, 1);
            indexManager.updateIndex(index, updatedPointer);

            Optional<Pointer> updatedRetrievedPointer = indexManager.getIndex(index);
            Assertions.assertTrue(updatedRetrievedPointer.isPresent());
            Assertions.assertEquals(updatedPointer, updatedRetrievedPointer.get());
        }
    }

    @Test
    public void test_purgeIndex() throws StorageException, IOException, InterruptedException, ExecutionException, BTreeException {
        IndexManager<String, Pointer> stringIndexManager = new TreeIndexManager<>(1, indexStorageManager, ImmediateIOSessionFactory.getInstance(), stringDbConfig,
                stringKBinaryObjectFactory, vBinaryObjectFactory, new DefaultNodeFactory<>(stringKBinaryObjectFactory, vBinaryObjectFactory));

        Map<Integer, Pointer> pointerMap = new HashMap<>();

        for (int index = 1; index <= 3; index++) {
            String stringIndex = "index" + index;
            Pointer pointer = new Pointer(Pointer.TYPE_DATA, Pointer.BYTES * (index - 1) + index, 1);
            stringIndexManager.addIndex(stringIndex, pointer);

            pointerMap.put(index, pointer);
        }

        indexStorageManager.purgeIndex(1);

        for (int index = 1; index <= 3; index++) {
            String stringIndex = "index" + index;
            Optional<Pointer> indexPointer = stringIndexManager.getIndex(stringIndex);
            Assertions.assertTrue(indexPointer.isEmpty());
        }
    }
}