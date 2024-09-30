package org.elece.index;

import org.elece.config.DbConfig;
import org.elece.config.DefaultDbConfigBuilder;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.storage.StorageException;
import org.elece.memory.Pointer;
import org.elece.memory.data.BinaryObjectFactory;
import org.elece.memory.data.PointerBinaryObject;
import org.elece.memory.tree.node.DefaultNodeFactory;
import org.elece.serializer.IntegerSerializer;
import org.elece.serializer.StringSerializer;
import org.elece.sql.db.schema.model.builder.ColumnBuilder;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.storage.file.DefaultFileHandlerFactory;
import org.elece.storage.file.UnrestrictedFileHandlerPool;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.OrganizedIndexStorageManager;
import org.elece.storage.index.header.DefaultIndexHeaderManagerFactory;
import org.elece.storage.index.session.factory.AtomicIOSessionFactory;
import org.elece.utils.BTreeUtils;
import org.elece.utils.BinaryUtils;
import org.elece.utils.FileTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.elece.storage.index.AbstractIndexStorageManager.INDEX_FILE_NAME;

class TreeIndexManagerTest {
    private Path dbPath;
    private DbConfig integerDbConfig;
    private DbConfig stringDbConfig;

    private IndexStorageManager indexStorageManager;

    private BinaryObjectFactory<Integer> integerKBinaryObjectFactory;
    private BinaryObjectFactory<String> stringKBinaryObjectFactory;
    private PointerBinaryObject.Factory pointerVBinaryObjectFactory;

    @BeforeEach
    public void setUp() throws IOException, StorageException {
        dbPath = Files.createTempDirectory("Tree_Index_Manager_Test_Case");

        integerKBinaryObjectFactory = new IntegerSerializer().getBinaryObjectFactory(ColumnBuilder.builder().setSqlType(SqlType.intType).build());
        stringKBinaryObjectFactory = new StringSerializer.StringBinaryObjectFactory(ColumnBuilder.builder().setSqlType(SqlType.varchar(255)).build(), new StringSerializer());

        integerDbConfig = DefaultDbConfigBuilder.builder()
                .setBaseDbPath(dbPath.toString())
                .setbTreeDegree(4)
                .setbTreeGrowthNodeAllocationCount(2)
                .setbTreeMaxFileSize(4L * BTreeUtils.calculateBPlusTreeSize(4, integerKBinaryObjectFactory.size(), PointerBinaryObject.BYTES))
                .setIOSessionStrategy(DbConfig.IOSessionStrategy.IMMEDIATE)
                .build();

        stringDbConfig = DefaultDbConfigBuilder.builder()
                .setBaseDbPath(dbPath.toString())
                .setbTreeDegree(4)
                .setbTreeGrowthNodeAllocationCount(2)
                .setbTreeMaxFileSize(4L * BTreeUtils.calculateBPlusTreeSize(4, stringKBinaryObjectFactory.size(), PointerBinaryObject.BYTES))
                .build();

        byte[] writingBytes = new byte[]{};
        Path indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        indexStorageManager = new OrganizedIndexStorageManager("test", new DefaultIndexHeaderManagerFactory(), integerDbConfig, new UnrestrictedFileHandlerPool(DefaultFileHandlerFactory.getInstance(), integerDbConfig));
        pointerVBinaryObjectFactory = new PointerBinaryObject.Factory();
    }

    @AfterEach
    public void destroy() throws IOException {
        FileTestUtils.deleteDirectory(dbPath.toString());
    }

    @Test
    public void test_tryToAddZero() {
        IndexManager<Integer, Pointer> indexManager = new TreeIndexManager<>(1, indexStorageManager, AtomicIOSessionFactory.getInstance(integerDbConfig), integerDbConfig,
                integerKBinaryObjectFactory, pointerVBinaryObjectFactory, new DefaultNodeFactory<>(integerKBinaryObjectFactory, pointerVBinaryObjectFactory));

        Assertions.assertThrows(SerializationException.class, () -> indexManager.addIndex(0, Pointer.empty()));
    }

    @Test
    public void test_addIntegerIndexes() throws BTreeException, StorageException, SerializationException {
        IndexManager<Integer, Pointer> indexManager = new TreeIndexManager<>(1, indexStorageManager, AtomicIOSessionFactory.getInstance(integerDbConfig), integerDbConfig,
                integerKBinaryObjectFactory, pointerVBinaryObjectFactory, new DefaultNodeFactory<>(integerKBinaryObjectFactory, pointerVBinaryObjectFactory));

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
    public void test_addStringIndexes() throws BTreeException, StorageException, SerializationException {
        IndexManager<String, Pointer> stringIndexManager = new TreeIndexManager<>(1, indexStorageManager, AtomicIOSessionFactory.getInstance(stringDbConfig), stringDbConfig,
                stringKBinaryObjectFactory, pointerVBinaryObjectFactory, new DefaultNodeFactory<>(stringKBinaryObjectFactory, pointerVBinaryObjectFactory));

        for (int index = 1; index <= 3; index++) {
            String stringIndex = "index" + index;
            Pointer pointer = new Pointer(Pointer.TYPE_DATA, Pointer.BYTES * (index - 1) + index, 1);
            stringIndexManager.addIndex(stringIndex, pointer);

            byte[] paddedStringIndexBytes = new byte[stringKBinaryObjectFactory.size()];

            byte[] bytes = BinaryUtils.stringToBytes(stringIndex);
            System.arraycopy(bytes, 0, paddedStringIndexBytes, 0, bytes.length);

            BinaryUtils.fillPadding(bytes.length, paddedStringIndexBytes.length, paddedStringIndexBytes);
            String paddedStringIndex = BinaryUtils.bytesToString(paddedStringIndexBytes, 0);

            Optional<Pointer> retrievedPointer = stringIndexManager.getIndex(paddedStringIndex);
            Assertions.assertTrue(retrievedPointer.isPresent());
            Assertions.assertEquals(pointer, retrievedPointer.get());

            Assertions.assertTrue(stringIndexManager.removeIndex(paddedStringIndex));
            Assertions.assertTrue(stringIndexManager.getIndex(paddedStringIndex).isEmpty());
        }
    }

    @Test
    public void test_updateIntegerIndexes() throws BTreeException, StorageException, SerializationException {
        IndexManager<Integer, Pointer> indexManager = new TreeIndexManager<>(1, indexStorageManager, AtomicIOSessionFactory.getInstance(stringDbConfig), integerDbConfig,
                integerKBinaryObjectFactory, pointerVBinaryObjectFactory, new DefaultNodeFactory<>(integerKBinaryObjectFactory, pointerVBinaryObjectFactory));

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
    public void test_purgeIndex() throws StorageException, IOException, InterruptedException, ExecutionException, BTreeException, SerializationException {
        IndexManager<String, Pointer> stringIndexManager = new TreeIndexManager<>(1, indexStorageManager, AtomicIOSessionFactory.getInstance(stringDbConfig), stringDbConfig,
                stringKBinaryObjectFactory, pointerVBinaryObjectFactory, new DefaultNodeFactory<>(stringKBinaryObjectFactory, pointerVBinaryObjectFactory));

        for (int index = 1; index <= 3; index++) {
            String stringIndex = "index" + index;
            Pointer pointer = new Pointer(Pointer.TYPE_DATA, Pointer.BYTES * (index - 1) + index, 1);
            stringIndexManager.addIndex(stringIndex, pointer);
        }

        indexStorageManager.purgeIndex(1);

        for (int index = 1; index <= 3; index++) {
            String stringIndex = "index" + index;
            Optional<Pointer> indexPointer = stringIndexManager.getIndex(stringIndex);
            Assertions.assertTrue(indexPointer.isEmpty());
        }
    }
}