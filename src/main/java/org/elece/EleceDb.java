package org.elece;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elece.config.DbConfig;
import org.elece.config.DefaultDbConfigBuilder;
import org.elece.exception.FileChannelException;
import org.elece.exception.InterruptedTaskException;
import org.elece.exception.ServerException;
import org.elece.exception.StorageException;
import org.elece.tcp.DefaultServer;
import org.elece.tcp.Server;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class EleceDb {
    private static final Logger logger = LogManager.getLogger(EleceDb.class);

    private static final Map<Class<?>, Function<String, ?>> clazzHandlers;

    static {
        clazzHandlers = new HashMap<>();

        clazzHandlers.put(Integer.class, Integer::parseInt);
        clazzHandlers.put(Long.class, Long::parseLong);
        clazzHandlers.put(String.class, String::trim);
        clazzHandlers.put(Boolean.class, Boolean::parseBoolean);

        clazzHandlers.put(TimeUnit.class, EleceDb::convertTimeUnit);
        clazzHandlers.put(DbConfig.IndexStorageManagerStrategy.class, EleceDb::convertIndexStorageManagerStrategy);
        clazzHandlers.put(DbConfig.FileHandlerStrategy.class, EleceDb::convertFileHandlerStrategy);
        clazzHandlers.put(DbConfig.SessionStrategy.class, EleceDb::convertSessionStrategy);
    }

    public static void main(String[] args) throws ServerException, InterruptedTaskException, StorageException,
                                                  FileChannelException {

        DbConfig dbConfig = buildDbConfig();
        Server server = new DefaultServer(dbConfig);

        logger.info("Starting server with config: {}", dbConfig);
        server.start();
    }

    private static DbConfig buildDbConfig() {
        DefaultDbConfigBuilder builder = DefaultDbConfigBuilder.builder();

        Integer port = getProperty("elece.db.port", Integer.class);
        if (Objects.nonNull(port)) {
            builder.setPort(port);
        }

        Integer poolCoreSize = getProperty("elece.db.pool.coreSize", Integer.class);
        if (Objects.nonNull(poolCoreSize)) {
            builder.setPoolCoreSize(poolCoreSize);
        }

        Integer poolMaxSize = getProperty("elece.db.pool.maxSize", Integer.class);
        if (Objects.nonNull(poolMaxSize)) {
            builder.setPoolMaxSize(poolMaxSize);
        }

        Long keepAliveTime = getProperty("elece.db.keepAliveTime", Long.class);
        if (Objects.nonNull(keepAliveTime)) {
            builder.setKeepAliveTime(keepAliveTime);
        }
        Integer fileDescriptorAcquisitionSize = getProperty("elece.db.fileDescriptorAcquisitionSize", Integer.class);
        if (Objects.nonNull(fileDescriptorAcquisitionSize)) {
            builder.setFileDescriptorAcquisitionSize(fileDescriptorAcquisitionSize);
        }

        Long closeTimeoutTime = getProperty("elece.db.closeTimeoutTime", Long.class);
        if (Objects.nonNull(closeTimeoutTime)) {
            builder.setCloseTimeoutTime(closeTimeoutTime);
        }

        Long acquisitionTimeoutTime = getProperty("elece.db.acquisitionTimeoutTime", Long.class);
        if (Objects.nonNull(acquisitionTimeoutTime)) {
            builder.setAcquisitionTimeoutTime(acquisitionTimeoutTime);
        }

        Integer timeoutUnit = getProperty("elece.db.timeoutUnit", Integer.class);
        if (Objects.nonNull(timeoutUnit)) {
            builder.setTimeoutUnit(TimeUnit.values()[timeoutUnit]);
        }

        Integer bTreeDegree = getProperty("elece.db.btree.degree", Integer.class);
        if (Objects.nonNull(bTreeDegree)) {
            builder.setBTreeDegree(bTreeDegree);
        }

        Integer bTreeGrowthNodeAllocationCount = getProperty("elece.db.btree.growthNodeAllocationCount", Integer.class);
        if (Objects.nonNull(bTreeGrowthNodeAllocationCount)) {
            builder.setBTreeGrowthNodeAllocationCount(bTreeGrowthNodeAllocationCount);
        }

        String baseDbPath = getProperty("elece.db.baseDbPath", String.class);
        if (Objects.nonNull(baseDbPath)) {
            builder.setBaseDbPath(baseDbPath);
        }

        Long bTreeMaxFileSize = getProperty("elece.db.btree.maxFileSize", Long.class);
        if (Objects.nonNull(bTreeMaxFileSize)) {
            builder.setBTreeMaxFileSize(bTreeMaxFileSize);
        }

        DbConfig.IndexStorageManagerStrategy indexStorageManagerStrategy = getProperty("elece.db.indexStorageManagerStrategy", DbConfig.IndexStorageManagerStrategy.class);
        if (Objects.nonNull(indexStorageManagerStrategy)) {
            builder.setIndexStorageManagerStrategy(indexStorageManagerStrategy);
        }

        DbConfig.FileHandlerStrategy fileHandlerStrategy = getProperty("elece.db.fileHandlerStrategy", DbConfig.FileHandlerStrategy.class);
        if (Objects.nonNull(fileHandlerStrategy)) {
            builder.setFileHandlerStrategy(fileHandlerStrategy);
        }

        Integer fileHandlerPoolThreads = getProperty("elece.db.fileHandlerPoolThreads", Integer.class);
        if (Objects.nonNull(fileHandlerPoolThreads)) {
            builder.setFileHandlerPoolThreads(fileHandlerPoolThreads);
        }

        DbConfig.SessionStrategy sessionStrategy = getProperty("elece.db.sessionStrategy", DbConfig.SessionStrategy.class);
        if (Objects.nonNull(sessionStrategy)) {
            builder.setSessionStrategy(sessionStrategy);
        }

        Integer dbPageSize = getProperty("elece.db.dbPageSize", Integer.class);
        if (Objects.nonNull(dbPageSize)) {
            builder.setDbPageSize(dbPageSize);
        }

        Integer dbPageMaxFileSize = getProperty("elece.db.dbPageMaxFileSize", Integer.class);
        if (Objects.nonNull(dbPageMaxFileSize)) {
            builder.setDbPageMaxFileSize(dbPageMaxFileSize);
        }

        Integer dbPageBufferSize = getProperty("elece.db.dbPageBufferSize", Integer.class);
        if (Objects.nonNull(dbPageBufferSize)) {
            builder.setDbPageBufferSize(dbPageBufferSize);
        }

        Integer dbQueryCacheSize = getProperty("elece.db.dbQueryCacheSize", Integer.class);
        if (Objects.nonNull(dbQueryCacheSize)) {
            builder.setDbQueryCacheSize(dbQueryCacheSize);
        }

        return builder.build();
    }

    private static <T> T getProperty(String name, Class<T> clazz) {
        if (Strings.isNullOrEmpty(name)) {
            throw new UnsupportedOperationException(String.format("Can't safely parse the property value to %s. Property value is null or empty", clazz));
        }

        String baseValue = System.getProperty(name);
        if (Strings.isNullOrEmpty(baseValue)) {
            return null;
        }

        if (!sanityCheck(clazz)) {
            return null;
        }

        try {
            return (T) clazzHandlers.get(clazz).apply(baseValue);
        } catch (Exception exception) {
            return null;
        }
    }

    private static <T> boolean sanityCheck(Class<T> clazz) {
        return clazzHandlers.containsKey(clazz);
    }

    private static DbConfig.SessionStrategy convertSessionStrategy(String strategy) {
        return switch (strategy) {
            case "COMMITTABLE":
                yield DbConfig.SessionStrategy.COMMITTABLE;
            case "IMMEDIATE":
                yield DbConfig.SessionStrategy.IMMEDIATE;
            default:
                yield null;
        };
    }

    private static DbConfig.FileHandlerStrategy convertFileHandlerStrategy(String strategy) {
        return switch (strategy) {
            case "LIMITED":
                yield DbConfig.FileHandlerStrategy.LIMITED;
            case "UNLIMITED":
                yield DbConfig.FileHandlerStrategy.UNLIMITED;
            default:
                yield null;
        };
    }

    private static DbConfig.IndexStorageManagerStrategy convertIndexStorageManagerStrategy(String strategy) {
        return switch (strategy) {
            case "ORGANIZED":
                yield DbConfig.IndexStorageManagerStrategy.ORGANIZED;
            case "COMPACT":
                yield DbConfig.IndexStorageManagerStrategy.COMPACT;
            default:
                yield null;
        };
    }

    private static TimeUnit convertTimeUnit(String timeUnit) {
        return switch (timeUnit) {
            case "SECONDS":
                yield TimeUnit.SECONDS;
            case "MILLISECONDS":
                yield TimeUnit.MILLISECONDS;
            case "MICROSECONDS":
                yield TimeUnit.MICROSECONDS;
            case "NANOSECONDS":
                yield TimeUnit.NANOSECONDS;
            case "MINUTES":
                yield TimeUnit.MINUTES;
            default:
                yield null;
        };
    }
}