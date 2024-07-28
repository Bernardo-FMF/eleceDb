package org.elece.sql.config;

public interface IDbConfig {
    int getPort();
    int getPoolMaxSize();
    int getPoolCoreSize();
    long getKeepAliveTime();
}
