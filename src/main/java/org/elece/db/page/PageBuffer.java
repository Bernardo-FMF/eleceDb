package org.elece.db.page;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.elece.config.DbConfig;
import org.elece.exception.*;
import org.elece.storage.file.FileChannel;
import org.elece.storage.file.FileHandlerPool;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * PageBuffer is responsible for managing a buffer of pages within a database.
 * It uses a cache to store pages and manages loaded pages using a ConcurrentHashMap.
 * This class ensures synchronized acquirement and release of pages, and handles creation
 * of new empty pages when necessary.
 */
public class PageBuffer {
    private final Cache<PageTitle, PageWrapper> buffer;
    private final PageFactory pageFactory;
    private final Map<PageTitle, PageWrapper> loadedPages;
    private final FileHandlerPool fileHandlerPool;
    private final DbConfig dbConfig;
    private final Function<Integer, Path> dbFileFunction;

    private volatile PageTitle lastPageTitle;

    public PageBuffer(DbConfig dbConfig, PageFactory pageFactory, FileHandlerPool fileHandlerPool,
                      Function<Integer, Path> dbFileFunction) {
        this.pageFactory = pageFactory;
        this.fileHandlerPool = fileHandlerPool;
        this.dbConfig = dbConfig;
        this.dbFileFunction = dbFileFunction;
        this.loadedPages = new ConcurrentHashMap<>();
        this.buffer = CacheBuilder
                .newBuilder()
                .maximumSize(dbConfig.getDbPageBufferSize())
                .initialCapacity(dbConfig.getDbPageBufferSize() / 2)
                .removalListener((RemovalListener<PageTitle, PageWrapper>) notification -> {
                    if (notification.getValue() == null) {
                        throw new RuntimeDbException(DbError.INTERNAL_DATABASE_ERROR, "Page is null");
                    }
                    if (notification.getValue().getRefCount() == 0) {
                        loadedPages.remove(notification.getKey());
                    }
                })
                .build();
    }

    /**
     * Acquires a Page associated with the given PageTitle. If the Page is not already loaded into memory,
     * it will be fetched and loaded. The reference count of the Page will also be incremented.
     *
     * @param title The title of the page to acquire, containing chunk and page number information.
     * @return The Page object associated with the given PageTitle.
     * @throws DbException              If there is an error while fetching the page.
     * @throws InterruptedTaskException If the task is interrupted during execution.
     * @throws StorageException         If there is an error during storage operations.
     * @throws FileChannelException     If there is an error related to file channel operations.
     */
    public synchronized Page acquire(PageTitle title) throws DbException, InterruptedTaskException, StorageException,
                                                             FileChannelException {
        PageWrapper pageWrapper = this.loadedPages.get(title);
        if (pageWrapper == null) {
            pageWrapper = this.buffer.getIfPresent(title);
            if (pageWrapper == null) {
                Page page = pageFactory.getPage(title);
                pageWrapper = new PageWrapper(page);
                buffer.put(title, pageWrapper);
                this.loadedPages.put(title, pageWrapper);
            }
        }

        pageWrapper.incrementRefCount();
        return pageWrapper.getPage();
    }

    /**
     * Releases the Page associated with the specified PageTitle. If the page is currently loaded,
     * its reference count is decremented. If after the decrement the reference count reaches zero,
     * the page is removed from the loaded pages.
     *
     * @param title The title of the page to release, containing chunk and page number information.
     */
    public synchronized void release(PageTitle title) {
        AtomicBoolean referred = new AtomicBoolean(false);
        this.loadedPages.computeIfPresent(title, (_, pageWrapper) -> {
            referred.set(true);
            pageWrapper.decrementRefCount();
            return pageWrapper;
        });
        if (referred.get()) {
            this.loadedPages.remove(title);
        }
    }

    /**
     * Retrieves the last buffered page from the database.
     * If the last page title is already known, it returns the appropriate page.
     * Otherwise, it determines the last available chunk of data, calculates the last page within that chunk,
     * updates the last page title, and returns the last page.
     *
     * @return An Optional containing the last Page within the buffer if successfully determined,
     * or an empty Optional if no pages are available.
     * @throws DbException              If there is a database-related error.
     * @throws StorageException         If there is an error related to storage.
     * @throws InterruptedTaskException If the task is interrupted.
     * @throws FileChannelException     If there is an error related to the file channel operations.
     */
    public Optional<Page> getBufferedLastPage() throws DbException, StorageException, InterruptedTaskException,
                                                       FileChannelException {
        if (this.lastPageTitle != null) {
            return Optional.of(acquire(this.lastPageTitle));
        }

        int lastChunk = -1;

        while (Files.exists(dbFileFunction.apply(lastChunk + 1))) {
            lastChunk++;
        }

        if (lastChunk == -1) {
            return Optional.empty();
        }

        synchronized (this) {
            int pageNumber;
            FileChannel fileChannel = fileHandlerPool.acquireFileHandler(dbFileFunction.apply(lastChunk));
            pageNumber = (int) (fileChannel.size() / this.dbConfig.getDbPageSize());

            fileHandlerPool.releaseFileHandler(dbFileFunction.apply(lastChunk));

            this.lastPageTitle = new PageTitle(lastChunk, pageNumber);

            return Optional.of(acquire(this.lastPageTitle));
        }
    }

    /**
     * Creates and buffers a new Page object based on the chunk and page number information.
     * If the maximum file size constraint is reached, it starts a new chunk.
     * A new empty page is generated and then acquired.
     *
     * @return The newly created and acquired Page object.
     * @throws DbException              If there is an error related to the database operation.
     * @throws InterruptedTaskException If the thread executing the method is interrupted.
     * @throws StorageException         If there is an error during storage allocation or retrieval.
     * @throws FileChannelException     If there is an error related to file channel operations.
     */
    public Page getBufferedNewPage() throws DbException, InterruptedTaskException, StorageException,
                                            FileChannelException {
        PageTitle tempLastPageTitle = this.lastPageTitle;

        int chunk = 0;
        int pageNumber = 0;

        if (tempLastPageTitle != null) {
            chunk = tempLastPageTitle.getChunk();
            pageNumber = tempLastPageTitle.getPageNumber() + 1;
        }

        if (this.dbConfig.getDbPageMaxFileSize() != DbConfig.UNLIMITED_FILE_SIZE && (long) pageNumber * this.dbConfig.getDbPageSize() > this.dbConfig.getDbPageMaxFileSize()) {
            chunk += 1;
        }

        this.generateNewEmptyPage(chunk);

        this.lastPageTitle = new PageTitle(chunk, pageNumber);
        return this.acquire(this.lastPageTitle);
    }


    /**
     * Generates a new empty page in the file associated with the given chunk number.
     * Allocates space for the new page using the configured database page size.
     *
     * @param chunk The chunk number for which a new empty page will be generated.
     * @throws InterruptedTaskException If the task is interrupted during execution.
     * @throws StorageException         If there is an error related to storage operations.
     * @throws FileChannelException     If there is an error related to file channel operations.
     */
    private void generateNewEmptyPage(int chunk) throws InterruptedTaskException, StorageException,
                                                        FileChannelException {
        int size = this.dbConfig.getDbPageSize();
        Path path = dbFileFunction.apply(chunk);
        FileChannel fileChannel = this.fileHandlerPool.acquireFileHandler(path);
        try {
            fileChannel.allocate(size);
        } finally {
            this.fileHandlerPool.releaseFileHandler(path);
        }
    }
}
