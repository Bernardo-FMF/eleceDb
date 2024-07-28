package org.elece.sql.config;

public class DbConfig implements IDbConfig {
    private final int port;
    private final int poolCoreSize;
    private final int poolMaxSize;
    private final long keepAliveTime;

    public DbConfig(int port, int poolCoreSize, int poolMaxSize, long keepAliveTime) {
        this.port = port;
        this.poolCoreSize = poolCoreSize;
        this.poolMaxSize = poolMaxSize;
        this.keepAliveTime = keepAliveTime;
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

    public int getPoolCoreSize() {
        return poolCoreSize;
    }
}
