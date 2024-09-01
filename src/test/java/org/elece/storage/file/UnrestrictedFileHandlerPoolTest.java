package org.elece.storage.file;

import org.elece.config.DbConfig;
import org.elece.config.DefaultDbConfigBuilder;
import org.elece.exception.storage.StorageException;
import org.elece.utils.FileTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

class UnrestrictedFileHandlerPoolTest {
    private UnrestrictedFileHandlerPool fileHandlerPool;
    private Path dbPath;

    @BeforeEach
    public void setup() throws IOException {
        dbPath = Files.createTempDirectory("Restricted_File_Handler_Pool_Test_Case");
        DbConfig dbConfig = DefaultDbConfigBuilder.builder()
                .setFileDescriptorAcquisitionSize(1)
                .setAcquisitionTimeoutTime(3)
                .setCloseTimeoutTime(3)
                .setTimeoutUnit(TimeUnit.SECONDS)
                .setFileHandlerPoolThreads(1)
                .setBaseDbPath(dbPath.toString())
                .build();

        fileHandlerPool = new UnrestrictedFileHandlerPool(DefaultFileHandlerFactory.getInstance(dbConfig.getFileHandlerPoolThreads()), dbConfig);
    }

    @AfterEach
    public void destroy() throws IOException {
        FileTestUtils.deleteDirectory(dbPath.toString());
    }

    @Test
    public void test_acquireFileHandler_newHandler() throws StorageException {
        String filename1 = "file1.bin";

        Path file1 = Path.of(dbPath.toString(), filename1);

        fileHandlerPool.acquireFileHandler(file1);

        fileHandlerPool.releaseFileHandler(file1);

        fileHandlerPool.closeAll();
    }

    @Test
    public void test_acquireFileHandler_existingHandler() throws StorageException {
        String filename1 = "file1.bin";

        Path file1 = Path.of(dbPath.toString(), filename1);

        fileHandlerPool.acquireFileHandler(file1);
        fileHandlerPool.acquireFileHandler(file1);

        fileHandlerPool.releaseFileHandler(file1);
        fileHandlerPool.releaseFileHandler(file1);

        fileHandlerPool.closeAll();
    }
}