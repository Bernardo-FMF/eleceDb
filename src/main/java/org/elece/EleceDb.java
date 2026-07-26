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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class EleceDb {
    private static final Logger logger = LogManager.getLogger(EleceDb.class);

    private static final Map<Class<?>, Function<String, ?>> clazzHandlers;

    static {
        clazzHandlers = new HashMap<>();

        clazzHandlers.put(Integer.class, Integer::parseInt);
        clazzHandlers.put(Long.class, Long::parseLong);
        clazzHandlers.put(Double.class, Double::parseDouble);
        clazzHandlers.put(String.class, String::trim);
        clazzHandlers.put(Boolean.class, Boolean::parseBoolean);

        clazzHandlers.put(TimeUnit.class, enumParser(TimeUnit.class));
        clazzHandlers.put(DbConfig.IndexStorageManagerStrategy.class, enumParser(DbConfig.IndexStorageManagerStrategy.class));
        clazzHandlers.put(DbConfig.FileHandlerStrategy.class, enumParser(DbConfig.FileHandlerStrategy.class));
        clazzHandlers.put(DbConfig.SessionStrategy.class, enumParser(DbConfig.SessionStrategy.class));
    }

    public static void main(String[] args) throws ServerException, InterruptedTaskException, StorageException,
            FileChannelException {
        DbConfig dbConfig = buildDbConfig();
        Server server = new DefaultServer(dbConfig);

        logger.info("Starting server with config: {}", dbConfig);
        try {
            server.start();
        } catch (ServerException | InterruptedTaskException | StorageException | FileChannelException exception) {
            logger.error("Encountered error, closing server", exception);
            throw exception;
        }
    }

    private static DbConfig buildDbConfig() {
        DefaultDbConfigBuilder builder = DefaultDbConfigBuilder.builder();

        applyProperty(builder, "elece.db.port", Integer.class, DefaultDbConfigBuilder::setPort);
        applyProperty(builder, "elece.db.pool.coreSize", Integer.class, DefaultDbConfigBuilder::setPoolCoreSize);
        applyProperty(builder, "elece.db.pool.maxSize", Integer.class, DefaultDbConfigBuilder::setPoolMaxSize);
        applyProperty(builder, "elece.db.keepAliveTime", Long.class, DefaultDbConfigBuilder::setKeepAliveTime);
        applyProperty(builder, "elece.db.fileDescriptorAcquisitionSize", Integer.class, DefaultDbConfigBuilder::setFileDescriptorAcquisitionSize);
        applyProperty(builder, "elece.db.closeTimeoutTime", Long.class, DefaultDbConfigBuilder::setCloseTimeoutTime);
        applyProperty(builder, "elece.db.acquisitionTimeoutTime", Long.class, DefaultDbConfigBuilder::setAcquisitionTimeoutTime);
        applyProperty(builder, "elece.db.timeoutUnit", TimeUnit.class, DefaultDbConfigBuilder::setTimeoutUnit);
        applyProperty(builder, "elece.db.btree.degree", Integer.class, DefaultDbConfigBuilder::setBTreeDegree);
        applyProperty(builder, "elece.db.btree.growthNodeAllocationCount", Integer.class, DefaultDbConfigBuilder::setBTreeGrowthNodeAllocationCount);
        applyProperty(builder, "elece.db.baseDbPath", String.class, DefaultDbConfigBuilder::setBaseDbPath);
        applyProperty(builder, "elece.db.btree.maxFileSize", Long.class, DefaultDbConfigBuilder::setBTreeMaxFileSize);
        applyProperty(builder, "elece.db.indexStorageManagerStrategy", DbConfig.IndexStorageManagerStrategy.class, DefaultDbConfigBuilder::setIndexStorageManagerStrategy);
        applyProperty(builder, "elece.db.fileHandlerStrategy", DbConfig.FileHandlerStrategy.class, DefaultDbConfigBuilder::setFileHandlerStrategy);
        applyProperty(builder, "elece.db.fileHandlerPoolThreads", Integer.class, DefaultDbConfigBuilder::setFileHandlerPoolThreads);
        applyProperty(builder, "elece.db.sessionStrategy", DbConfig.SessionStrategy.class, DefaultDbConfigBuilder::setSessionStrategy);
        applyProperty(builder, "elece.db.dbPageSize", Integer.class, DefaultDbConfigBuilder::setDbPageSize);
        applyProperty(builder, "elece.db.dbPageMaxFileSize", Integer.class, DefaultDbConfigBuilder::setDbPageMaxFileSize);
        applyProperty(builder, "elece.db.dbPageBufferSize", Integer.class, DefaultDbConfigBuilder::setDbPageBufferSize);
        applyProperty(builder, "elece.db.dbQueryCacheSize", Integer.class, DefaultDbConfigBuilder::setDbQueryCacheSize);
        applyProperty(builder, "elece.db.bloom.enabled", Boolean.class, DefaultDbConfigBuilder::setBloomFilterEnabled);
        applyProperty(builder, "elece.db.bloom.falsePositiveRate", Double.class, DefaultDbConfigBuilder::setBloomFilterFalsePositiveRate);
        applyProperty(builder, "elece.db.bloom.expectedInsertions", Integer.class, DefaultDbConfigBuilder::setBloomFilterExpectedInsertions);

        return builder.build();
    }

    /**
     * Reads a single configuration property and, when present and valid, applies it to the builder through
     * the given setter. Absent or invalid values leave the builder's default in place.
     */
    private static <T> void applyProperty(DefaultDbConfigBuilder builder, String name, Class<T> clazz,
                                          BiConsumer<DefaultDbConfigBuilder, T> setter) {
        T value = getProperty(name, clazz);
        if (Objects.nonNull(value)) {
            setter.accept(builder, value);
        }
    }

    public static <T> T getProperty(String name, Class<T> clazz) {
        if (Strings.isNullOrEmpty(name)) {
            throw new UnsupportedOperationException(String.format("Can't safely parse the property value to %s. Property name is null or empty", clazz));
        }

        String rawValue = resolveRawValue(name);
        if (Strings.isNullOrEmpty(rawValue)) {
            return null;
        }

        if (!sanityCheck(clazz)) {
            return null;
        }

        try {
            return clazz.cast(clazzHandlers.get(clazz).apply(rawValue));
        } catch (Exception exception) {
            logger.warn("Ignoring invalid value '{}' for config '{}'; falling back to the default", rawValue, name);
            return null;
        }
    }

    /**
     * Resolves a raw configuration value, preferring a JVM system property ({@code -Delece.db.port=...}) and
     * falling back to the equivalent environment variable ({@code ELECE_DB_PORT}). The system property wins so
     * existing overrides keep working, while the environment-variable form makes {@code docker run -e ...} work.
     */
    private static String resolveRawValue(String propertyName) {
        String systemProperty = System.getProperty(propertyName);
        if (!Strings.isNullOrEmpty(systemProperty)) {
            return systemProperty;
        }
        return System.getenv(toEnvVariableName(propertyName));
    }

    /**
     * Maps a dotted property name to its environment-variable form, e.g.
     * {@code elece.db.pool.coreSize} becomes {@code ELECE_DB_POOL_CORE_SIZE}.
     */
    private static String toEnvVariableName(String propertyName) {
        return propertyName
                .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
                .replace('.', '_')
                .toUpperCase(Locale.ROOT);
    }

    private static <T> boolean sanityCheck(Class<T> clazz) {
        return clazzHandlers.containsKey(clazz);
    }

    /**
     * Builds a parser that resolves an enum constant by name, throwing {@link IllegalArgumentException} for an
     * unknown value so the caller logs it and falls back to the default.
     */
    private static <E extends Enum<E>> Function<String, E> enumParser(Class<E> enumClass) {
        return value -> Enum.valueOf(enumClass, value);
    }
}
