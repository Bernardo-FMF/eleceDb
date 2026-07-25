package org.elece.thread;

import org.elece.config.DbConfig;

import java.util.concurrent.*;

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

    /**
     * Attempts to hand the task to the pool for execution.
     *
     * @return {@code true} if the task was accepted; {@code false} if the pool is saturated
     * (all threads up to the configured max size are busy) and the task was rejected.
     */
    public boolean execute(T task) {
        try {
            executor.execute(task);
            return true;
        } catch (RejectedExecutionException exception) {
            return false;
        }
    }
}
