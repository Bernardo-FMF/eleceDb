package org.elece.config;

import java.util.concurrent.TimeUnit;

public interface DbConfig {
    Integer UNLIMITED_FILE_SIZE = -1;

    int getPort();

    int getPoolMaxSize();

    int getPoolCoreSize();

    long getKeepAliveTime();

    int getFileDescriptorAcquisitionSize();

    long getCloseTimeoutTime();

    long getAcquisitionTimeoutTime();

    TimeUnit getTimeoutUnit();

    int getBTreeDegree();

    int getBTreeGrowthNodeAllocationCount();

    String getBaseDbPath();

    long getBTreeMaxFileSize();

    IndexStorageManagerStrategy getIndexStorageManagerStrategy();

    FileHandlerStrategy getFileHandlerStrategy();

    int getFileHandlerPoolThreads();

    IOSessionStrategy getIOSessionStrategy();

    int getDbPageSize();

    int getDbPageBufferSize();

    int getDbPageMaxFileSize();

    int getDbQueryCacheSize();

    enum IOSessionStrategy {
        COMMITTABLE, IMMEDIATE
    }

    enum IndexStorageManagerStrategy {
        ORGANIZED, COMPACT
    }

    enum FileHandlerStrategy {
        LIMITED, UNLIMITED
    }
}
