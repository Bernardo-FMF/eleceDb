package org.elece.config;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class DefaultDbConfigBuilder {
    private Integer port;
    private Integer poolCoreSize;
    private Integer poolMaxSize;
    private Long keepAliveTime;
    private Integer fileDescriptorAcquisitionSize;
    private Long closeTimeoutTime;
    private Long acquisitionTimeoutTime;
    private TimeUnit timeoutUnit;
    private Integer bTreeDegree;
    private Integer bTreeGrowthNodeAllocationCount;
    private String baseDbPath;
    private Long bTreeMaxFileSize;
    private DbConfig.IndexStorageManagerStrategy indexStorageManagerStrategy;
    private DbConfig.FileHandlerStrategy fileHandlerStrategy;
    private Integer fileHandlerPoolThreads;
    private DbConfig.SessionStrategy sessionStrategy;
    private Integer dbPageSize;
    private Integer dbPageBufferSize;
    private Integer dbPageMaxFileSize;
    private Integer dbQueryCacheSize;

    private DefaultDbConfigBuilder() {
        // private constructor
    }

    public static DefaultDbConfigBuilder builder() {
        return new DefaultDbConfigBuilder();
    }

    public DefaultDbConfigBuilder setPort(int port) {
        this.port = port;
        return this;
    }

    public DefaultDbConfigBuilder setPoolCoreSize(int poolCoreSize) {
        this.poolCoreSize = poolCoreSize;
        return this;
    }

    public DefaultDbConfigBuilder setPoolMaxSize(int poolMaxSize) {
        this.poolMaxSize = poolMaxSize;
        return this;
    }

    public DefaultDbConfigBuilder setKeepAliveTime(long keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
        return this;
    }

    public DefaultDbConfigBuilder setFileDescriptorAcquisitionSize(int fileDescriptorAcquisitionSize) {
        this.fileDescriptorAcquisitionSize = fileDescriptorAcquisitionSize;
        return this;
    }

    public DefaultDbConfigBuilder setCloseTimeoutTime(long closeTimeoutTime) {
        this.closeTimeoutTime = closeTimeoutTime;
        return this;
    }

    public DefaultDbConfigBuilder setAcquisitionTimeoutTime(long acquisitionTimeoutTime) {
        this.acquisitionTimeoutTime = acquisitionTimeoutTime;
        return this;
    }

    public DefaultDbConfigBuilder setTimeoutUnit(TimeUnit timeoutUnit) {
        this.timeoutUnit = timeoutUnit;
        return this;
    }

    public DefaultDbConfigBuilder setBTreeDegree(int bTreeDegree) {
        this.bTreeDegree = bTreeDegree;
        return this;
    }

    public DefaultDbConfigBuilder setBTreeGrowthNodeAllocationCount(int bTreeGrowthNodeAllocationCount) {
        this.bTreeGrowthNodeAllocationCount = bTreeGrowthNodeAllocationCount;
        return this;
    }

    public DefaultDbConfigBuilder setBaseDbPath(String baseDbPath) {
        this.baseDbPath = baseDbPath;
        return this;
    }

    public DefaultDbConfigBuilder setBTreeMaxFileSize(long bTreeMaxFileSize) {
        this.bTreeMaxFileSize = bTreeMaxFileSize;
        return this;
    }

    public DefaultDbConfigBuilder setIndexStorageManagerStrategy(DbConfig.IndexStorageManagerStrategy indexStorageManagerStrategy) {
        this.indexStorageManagerStrategy = indexStorageManagerStrategy;
        return this;
    }

    public DefaultDbConfigBuilder setFileHandlerStrategy(DbConfig.FileHandlerStrategy fileHandlerStrategy) {
        this.fileHandlerStrategy = fileHandlerStrategy;
        return this;
    }

    public DefaultDbConfigBuilder setFileHandlerPoolThreads(int fileHandlerPoolThreads) {
        this.fileHandlerPoolThreads = fileHandlerPoolThreads;
        return this;
    }

    public DefaultDbConfigBuilder setSessionStrategy(DbConfig.SessionStrategy sessionStrategy) {
        this.sessionStrategy = sessionStrategy;
        return this;
    }

    public DefaultDbConfigBuilder setDbPageSize(Integer dbPageSize) {
        this.dbPageSize = dbPageSize;
        return this;
    }

    public DefaultDbConfigBuilder setDbPageMaxFileSize(Integer dbPageMaxFileSize) {
        this.dbPageMaxFileSize = dbPageMaxFileSize;
        return this;
    }

    public DefaultDbConfigBuilder setDbPageBufferSize(Integer dbPageBufferSize) {
        this.dbPageBufferSize = dbPageBufferSize;
        return this;
    }

    public DefaultDbConfigBuilder setDbQueryCacheSize(Integer dbQueryCacheSize) {
        this.dbQueryCacheSize = dbQueryCacheSize;
        return this;
    }

    private int getPort() {
        return Objects.requireNonNullElse(port, 3000);
    }

    private int getPoolCoreSize() {
        return Objects.requireNonNullElse(poolCoreSize, 5);
    }

    private int getPoolMaxSize() {
        return Objects.requireNonNullElse(poolMaxSize, 20);
    }

    private long getKeepAliveTime() {
        return Objects.requireNonNullElse(keepAliveTime, 100L);
    }

    private int getFileDescriptorAcquisitionSize() {
        return Objects.requireNonNullElse(fileDescriptorAcquisitionSize, 20);
    }

    private long getCloseTimeoutTime() {
        return Objects.requireNonNullElse(closeTimeoutTime, 5L);
    }

    private long getAcquisitionTimeoutTime() {
        return Objects.requireNonNullElse(acquisitionTimeoutTime, 10L);
    }

    private TimeUnit getTimeoutUnit() {
        return Objects.requireNonNullElse(timeoutUnit, TimeUnit.SECONDS);
    }

    private int getbTreeDegree() {
        return Objects.requireNonNullElse(bTreeDegree, 10);
    }

    private int getbTreeGrowthNodeAllocationCount() {
        return Objects.requireNonNullElse(bTreeGrowthNodeAllocationCount, 20);
    }

    private String getBaseDbPath() {
        return Objects.requireNonNullElse(baseDbPath, "/temp");
    }

    private long getBTreeMaxFileSize() {
        return Objects.requireNonNullElse(bTreeMaxFileSize, -1L);
    }

    private DbConfig.IndexStorageManagerStrategy getIndexStorageManagerStrategy() {
        return Objects.requireNonNullElse(indexStorageManagerStrategy, DbConfig.IndexStorageManagerStrategy.COMPACT);
    }

    private DbConfig.FileHandlerStrategy getFileHandlerStrategy() {
        return Objects.requireNonNullElse(fileHandlerStrategy, DbConfig.FileHandlerStrategy.UNLIMITED);
    }

    private int getFileHandlerPoolThreads() {
        return Objects.requireNonNullElse(fileHandlerPoolThreads, 10);
    }

    private DbConfig.SessionStrategy getIOSessionStrategy() {
        return Objects.requireNonNullElse(sessionStrategy, DbConfig.SessionStrategy.IMMEDIATE);
    }

    private int getDbPageSize() {
        return Objects.requireNonNullElse(dbPageSize, 64000);
    }

    private int getDbPageBufferSize() {
        return Objects.requireNonNullElse(dbPageBufferSize, 100);
    }

    private int getDbPageMaxFileSize() {
        return Objects.requireNonNullElse(dbPageMaxFileSize, DbConfig.UNLIMITED_FILE_SIZE);
    }

    private int getDbQueryCacheSize() {
        return Objects.requireNonNullElse(dbQueryCacheSize, 50);
    }

    public DefaultDbConfig build() {
        return new DefaultDbConfig(getPort(), getPoolCoreSize(), getPoolMaxSize(), getKeepAliveTime(),
                getFileDescriptorAcquisitionSize(), getCloseTimeoutTime(), getAcquisitionTimeoutTime(), getTimeoutUnit(),
                getbTreeDegree(), getbTreeGrowthNodeAllocationCount(), getBaseDbPath(), getBTreeMaxFileSize(),
                getIndexStorageManagerStrategy(), getFileHandlerStrategy(), getFileHandlerPoolThreads(), getIOSessionStrategy(),
                getDbPageSize(), getDbPageBufferSize(), getDbPageMaxFileSize(), getDbQueryCacheSize());
    }
}