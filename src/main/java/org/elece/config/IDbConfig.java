package org.elece.config;

import java.util.concurrent.TimeUnit;

public interface IDbConfig {
    int getPort();
    int getPoolMaxSize();
    int getPoolCoreSize();
    long getKeepAliveTime();

    int getFileDescriptorAcquisitionSize();
    long getCloseTimeoutTime();
    long getAcquisitionTimeoutTime();
    TimeUnit getTimeoutUnit();
}
