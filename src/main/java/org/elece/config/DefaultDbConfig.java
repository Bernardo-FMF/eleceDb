package org.elece.config;

import java.util.concurrent.TimeUnit;

public class DefaultDbConfig implements DbConfig {
    private final int port;
    private final int poolCoreSize;
    private final int poolMaxSize;
    private final long keepAliveTime;
    private final int fileDescriptorAcquisitionSize;
    private final long closeTimeoutTime;
    private final long acquisitionTimeoutTime;
    private final TimeUnit timeoutUnit;
    private final int bTreeDegree;
    private final int bTreeGrowthNodeAllocationCount;
    private final String baseDbPath;
    private final long bTreeMaxFileSize;

    public DefaultDbConfig(int port, int poolCoreSize, int poolMaxSize, long keepAliveTime, int fileDescriptorAcquisitionSize, long closeTimeoutTime, long acquisitionTimeoutTime, TimeUnit timeoutUnit, int bTreeDegree, int bTreeGrowthNodeAllocationCount, String baseDbPath, long bTreeMaxFileSize) {
        this.port = port;
        this.poolCoreSize = poolCoreSize;
        this.poolMaxSize = poolMaxSize;
        this.keepAliveTime = keepAliveTime;
        this.fileDescriptorAcquisitionSize = fileDescriptorAcquisitionSize;
        this.closeTimeoutTime = closeTimeoutTime;
        this.acquisitionTimeoutTime = acquisitionTimeoutTime;
        this.timeoutUnit = timeoutUnit;
        this.bTreeDegree = bTreeDegree;
        this.bTreeGrowthNodeAllocationCount = bTreeGrowthNodeAllocationCount;
        this.baseDbPath = baseDbPath;
        this.bTreeMaxFileSize = bTreeMaxFileSize;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public int getPoolMaxSize() {
        return poolMaxSize;
    }

    @Override
    public int getPoolCoreSize() {
        return poolCoreSize;
    }

    @Override
    public long getKeepAliveTime() {
        return keepAliveTime;
    }

    @Override
    public int getFileDescriptorAcquisitionSize() {
        return fileDescriptorAcquisitionSize;
    }

    @Override
    public long getCloseTimeoutTime() {
        return closeTimeoutTime;
    }

    @Override
    public long getAcquisitionTimeoutTime() {
        return acquisitionTimeoutTime;
    }

    @Override
    public TimeUnit getTimeoutUnit() {
        return timeoutUnit;
    }

    @Override
    public int getBTreeDegree() {
        return bTreeDegree;
    }

    @Override
    public int getBTreeGrowthNodeAllocationCount() {
        return bTreeGrowthNodeAllocationCount;
    }

    @Override
    public String getBaseDbPath() {
        return baseDbPath;
    }

    @Override
    public long getBTreeMaxFileSize() {
        return bTreeMaxFileSize;
    }
}
