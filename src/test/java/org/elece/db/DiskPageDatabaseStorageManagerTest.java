package org.elece.db;

import org.elece.config.DbConfig;
import org.elece.config.DefaultDbConfigBuilder;
import org.elece.exception.*;
import org.elece.memory.Pointer;
import org.elece.memory.tree.node.LeafTreeNode;
import org.elece.storage.file.DefaultFileHandlerFactory;
import org.elece.storage.file.UnrestrictedFileHandlerPool;
import org.elece.utils.BinaryUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DiskPageDatabaseStorageManagerTest {
    private DiskPageDatabaseStorageManager diskPageDatabaseStorageManager;

    @BeforeEach
    public void setup() throws IOException {
        DbConfig dbConfig = DefaultDbConfigBuilder.builder()
                .setBaseDbPath(Files.createTempDirectory("Disk_Page_Database_Storage_Manager_Test").toString())
                .setDbPageSize(64000)
                .setDbPageBufferSize(100)
                .setDbPageMaxFileSize(64000)
                .build();
        diskPageDatabaseStorageManager = new DiskPageDatabaseStorageManager(dbConfig, new UnrestrictedFileHandlerPool(DefaultFileHandlerFactory.getInstance(), dbConfig), new InMemoryReservedSlotTracer());
    }

    @Test
    void test_storeDeleteAndReuseObjects() throws DbException, StorageException, InterruptedTaskException,
                                                  FileChannelException {
        List<String> inputs = Arrays.asList("10", "100", "1000", "10000");
        List<Pointer> pointers = new ArrayList<>();

        for (int index = 0; index < inputs.size(); index++) {
            byte[] data = inputs.get(index).getBytes(StandardCharsets.UTF_8);
            Pointer pointer = this.diskPageDatabaseStorageManager.store(1, data);
            Assertions.assertNotNull(pointer);
            pointers.add(index, pointer);
        }

        for (int index = 0; index < inputs.size(); index++) {
            Pointer pointer = pointers.get(index);
            Optional<DbObject> optionalDbObject = this.diskPageDatabaseStorageManager.select(pointer);
            Assertions.assertTrue(optionalDbObject.isPresent());
            Assertions.assertTrue(optionalDbObject.get().isAlive());
            Assertions.assertEquals(inputs.get(index), BinaryUtils.bytesToString(optionalDbObject.get().getData(), 0));
        }

        for (Pointer pointer : pointers) {
            this.diskPageDatabaseStorageManager.remove(pointer);
            Optional<DbObject> optionalDbObject = this.diskPageDatabaseStorageManager.select(pointer);
            Assertions.assertTrue(optionalDbObject.isPresent());
            Assertions.assertFalse(optionalDbObject.get().isAlive());
        }

        for (int index = 0; index < inputs.size(); index++) {
            byte[] data = inputs.get(index).getBytes(StandardCharsets.UTF_8);
            Pointer pointer = this.diskPageDatabaseStorageManager.store(1, data);
            Assertions.assertEquals(pointers.get(index), pointer);
        }
    }

    @Test
    public void test_storeObjectsMultiThreaded() throws DbException, InterruptedException, InterruptedTaskException,
                                                        StorageException, FileChannelException {
        int cases = 20;

        try (ExecutorService executorService = Executors.newFixedThreadPool(cases)) {
            List<LeafTreeNode.KeyValue<String, Pointer>> keyValues = new CopyOnWriteArrayList<>();

            CountDownLatch countDownLatch = new CountDownLatch(cases);

            for (int index = 0; index < cases; index++) {
                executorService.submit(() -> {
                    Random random = new Random();
                    byte[] array = new byte[random.nextInt(100) + 1];
                    random.nextBytes(array);
                    String generatedString = new String(array, StandardCharsets.UTF_8);
                    try {
                        byte[] generatedStringBytes = BinaryUtils.stringToBytes(generatedString);
                        Pointer pointer = diskPageDatabaseStorageManager.store(1, generatedStringBytes);
                        keyValues.add(new LeafTreeNode.KeyValue<>(generatedString, pointer));
                    } catch (StorageException | DbException | InterruptedTaskException |
                             FileChannelException exception) {
                        throw new RuntimeDbException(exception.getDbError(), exception.getMessage());
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }

            countDownLatch.await();

            for (LeafTreeNode.KeyValue<String, Pointer> keyValue : keyValues) {
                Optional<DbObject> dbObject = this.diskPageDatabaseStorageManager.select(keyValue.value());
                Assertions.assertTrue(dbObject.isPresent());
                Assertions.assertTrue(dbObject.get().isAlive());
                String value = BinaryUtils.bytesToString(dbObject.get().getData(), 0);
                Assertions.assertEquals(keyValue.key(), value);
            }

            executorService.shutdownNow();
        }
    }
}