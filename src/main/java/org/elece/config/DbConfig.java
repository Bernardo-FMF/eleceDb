package org.elece.config;

import java.util.concurrent.TimeUnit;

public class DbConfig implements IDbConfig {
    private final int port;
    private final int poolCoreSize;
    private final int poolMaxSize;
    private final long keepAliveTime;
    private final int fileDescriptorAcquisitionSize;
    private final long closeTimeoutTime;
    private final long acquisitionTimeoutTime;
    private final TimeUnit timeoutUnit;

    public DbConfig(int port, int poolCoreSize, int poolMaxSize, long keepAliveTime, int fileDescriptorAcquisitionSize, long closeTimeoutTime, long acquisitionTimeoutTime, TimeUnit timeoutUnit) {
        this.port = port;
        this.poolCoreSize = poolCoreSize;
        this.poolMaxSize = poolMaxSize;
        this.keepAliveTime = keepAliveTime;
        this.fileDescriptorAcquisitionSize = fileDescriptorAcquisitionSize;
        this.closeTimeoutTime = closeTimeoutTime;
        this.acquisitionTimeoutTime = acquisitionTimeoutTime;
        this.timeoutUnit = timeoutUnit;
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

    public int getPoolCoreSize() {
        return poolCoreSize;
    }
}
