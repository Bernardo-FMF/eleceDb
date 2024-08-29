package org.elece.config;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

// TODO: Create constants for default values
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

    public DefaultDbConfigBuilder setbTreeDegree(int bTreeDegree) {
        this.bTreeDegree = bTreeDegree;
        return this;
    }

    public DefaultDbConfigBuilder setbTreeGrowthNodeAllocationCount(int bTreeGrowthNodeAllocationCount) {
        this.bTreeGrowthNodeAllocationCount = bTreeGrowthNodeAllocationCount;
        return this;
    }

    public DefaultDbConfigBuilder setBaseDbPath(String baseDbPath) {
        this.baseDbPath = baseDbPath;
        return this;
    }

    public DefaultDbConfigBuilder setbTreeMaxFileSize(long bTreeMaxFileSize) {
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

    private long getbTreeMaxFileSize() {
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

    public DefaultDbConfig createDefaultDbConfig() {
        return new DefaultDbConfig(getPort(), getPoolCoreSize(), getPoolMaxSize(), getKeepAliveTime(),
                getFileDescriptorAcquisitionSize(), getCloseTimeoutTime(), getAcquisitionTimeoutTime(), getTimeoutUnit(),
                getbTreeDegree(), getbTreeGrowthNodeAllocationCount(), getBaseDbPath(), getbTreeMaxFileSize(),
                getIndexStorageManagerStrategy(), getFileHandlerStrategy(), getFileHandlerPoolThreads());
    }
}