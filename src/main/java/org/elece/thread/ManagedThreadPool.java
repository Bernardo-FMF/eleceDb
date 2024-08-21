package org.elece.thread;

import org.elece.config.DbConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ManagedThreadPool<T extends Runnable> {
    private final ExecutorService executor;

    public ManagedThreadPool(DbConfig dbConfig) {
        this.executor = new ThreadPoolExecutor(
                dbConfig.getPoolCoreSize(),
                dbConfig.getPoolMaxSize(),
                dbConfig.getKeepAliveTime(),
                TimeUnit.SECONDS,
                new SynchronousQueue<>()
        );
    }

    public boolean isRunning() {
        return !executor.isShutdown() && !executor.isTerminated();
    }

    public void execute(T task) {
        executor.execute(task);
    }
}
