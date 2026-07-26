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
    private final IndexStorageManagerStrategy indexStorageManagerStrategy;
    private final FileHandlerStrategy fileHandlerStrategy;
    private final int fileHandlerPoolThreads;
    private final SessionStrategy sessionStrategy;
    private final int dbPageSize;
    private final int dbPageBufferSize;
    private final int dbPageMaxFileSize;
    private final int dbQueryCacheSize;
    private final boolean bloomFilterEnabled;
    private final double bloomFilterFalsePositiveRate;
    private final int bloomFilterExpectedInsertions;

    public DefaultDbConfig(int port, int poolCoreSize, int poolMaxSize, long keepAliveTime,
                           int fileDescriptorAcquisitionSize,
                           long closeTimeoutTime, long acquisitionTimeoutTime, TimeUnit timeoutUnit, int bTreeDegree,
                           int bTreeGrowthNodeAllocationCount, String baseDbPath, long bTreeMaxFileSize,
                           IndexStorageManagerStrategy indexStorageManagerStrategy,
                           FileHandlerStrategy fileHandlerStrategy,
                           int fileHandlerPoolThreads, SessionStrategy sessionStrategy, int dbPageSize,
                           int dbPageBufferSize,
                           int dbPageMaxFileSize, int dbQueryCacheSize, boolean bloomFilterEnabled,
                           double bloomFilterFalsePositiveRate, int bloomFilterExpectedInsertions) {
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
        this.indexStorageManagerStrategy = indexStorageManagerStrategy;
        this.fileHandlerStrategy = fileHandlerStrategy;
        this.fileHandlerPoolThreads = fileHandlerPoolThreads;
        this.sessionStrategy = sessionStrategy;
        this.dbPageSize = dbPageSize;
        this.dbPageBufferSize = dbPageBufferSize;
        this.dbPageMaxFileSize = dbPageMaxFileSize;
        this.dbQueryCacheSize = dbQueryCacheSize;
        this.bloomFilterEnabled = bloomFilterEnabled;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.bloomFilterExpectedInsertions = bloomFilterExpectedInsertions;
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

    @Override
    public IndexStorageManagerStrategy getIndexStorageManagerStrategy() {
        return indexStorageManagerStrategy;
    }

    @Override
    public FileHandlerStrategy getFileHandlerStrategy() {
        return fileHandlerStrategy;
    }

    @Override
    public int getFileHandlerPoolThreads() {
        return fileHandlerPoolThreads;
    }

    @Override
    public SessionStrategy getSessionStrategy() {
        return sessionStrategy;
    }

    @Override
    public int getDbPageSize() {
        return dbPageSize;
    }

    @Override
    public int getDbPageBufferSize() {
        return dbPageBufferSize;
    }

    @Override
    public int getDbPageMaxFileSize() {
        return dbPageMaxFileSize;
    }

    @Override
    public int getDbQueryCacheSize() {
        return dbQueryCacheSize;
    }

    @Override
    public boolean isBloomFilterEnabled() {
        return bloomFilterEnabled;
    }

    @Override
    public double getBloomFilterFalsePositiveRate() {
        return bloomFilterFalsePositiveRate;
    }

    @Override
    public int getBloomFilterExpectedInsertions() {
        return bloomFilterExpectedInsertions;
    }

    @Override
    public String toString() {
        return "DefaultDbConfig{" +
                "port=" + port +
                ", poolCoreSize=" + poolCoreSize +
                ", poolMaxSize=" + poolMaxSize +
                ", keepAliveTime=" + keepAliveTime +
                ", fileDescriptorAcquisitionSize=" + fileDescriptorAcquisitionSize +
                ", closeTimeoutTime=" + closeTimeoutTime +
                ", acquisitionTimeoutTime=" + acquisitionTimeoutTime +
                ", timeoutUnit=" + timeoutUnit +
                ", bTreeDegree=" + bTreeDegree +
                ", bTreeGrowthNodeAllocationCount=" + bTreeGrowthNodeAllocationCount +
                ", baseDbPath='" + baseDbPath + '\'' +
                ", bTreeMaxFileSize=" + bTreeMaxFileSize +
                ", indexStorageManagerStrategy=" + indexStorageManagerStrategy +
                ", fileHandlerStrategy=" + fileHandlerStrategy +
                ", fileHandlerPoolThreads=" + fileHandlerPoolThreads +
                ", sessionStrategy=" + sessionStrategy +
                ", dbPageSize=" + dbPageSize +
                ", dbPageBufferSize=" + dbPageBufferSize +
                ", dbPageMaxFileSize=" + dbPageMaxFileSize +
                ", dbQueryCacheSize=" + dbQueryCacheSize +
                ", bloomFilterEnabled=" + bloomFilterEnabled +
                ", bloomFilterFalsePositiveRate=" + bloomFilterFalsePositiveRate +
                ", bloomFilterExpectedInsertions=" + bloomFilterExpectedInsertions +
                '}';
    }
}
