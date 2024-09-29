package org.elece.config;

import java.util.concurrent.TimeUnit;

public interface DbConfig {
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
