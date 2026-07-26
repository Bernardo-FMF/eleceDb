package org.elece.config;

import java.util.concurrent.TimeUnit;

/**
 * Immutable {@link DbConfig} value holder.
 * <p>
 * Implemented as a {@code record} so the field list, canonical constructor, {@code equals}/{@code hashCode}
 * and {@code toString} stay in sync automatically as configuration options are added. Instances are produced
 * by {@link DefaultDbConfigBuilder}. The explicit {@code getX()} / {@code isX()} methods bridge the record's
 * component accessors to the {@link DbConfig} interface, whose accessors are {@code get}-prefixed.
 */
public record DefaultDbConfig(int port, int poolCoreSize, int poolMaxSize, long keepAliveTime,
                              int fileDescriptorAcquisitionSize, long closeTimeoutTime, long acquisitionTimeoutTime,
                              TimeUnit timeoutUnit, int bTreeDegree, int bTreeGrowthNodeAllocationCount,
                              String baseDbPath, long bTreeMaxFileSize,
                              DbConfig.IndexStorageManagerStrategy indexStorageManagerStrategy,
                              DbConfig.FileHandlerStrategy fileHandlerStrategy, int fileHandlerPoolThreads,
                              DbConfig.SessionStrategy sessionStrategy, int dbPageSize, int dbPageBufferSize,
                              int dbPageMaxFileSize, int dbQueryCacheSize, boolean bloomFilterEnabled,
                              double bloomFilterFalsePositiveRate, int bloomFilterExpectedInsertions)
        implements DbConfig {

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
}
