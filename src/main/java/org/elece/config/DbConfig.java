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
}
