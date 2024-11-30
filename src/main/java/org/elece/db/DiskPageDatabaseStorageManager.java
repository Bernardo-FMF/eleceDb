package org.elece.db;

import org.elece.config.DbConfig;
import org.elece.db.page.DefaultPageFactory;
import org.elece.db.page.Page;
import org.elece.db.page.PageBuffer;
import org.elece.db.page.PageTitle;
import org.elece.exception.*;
import org.elece.memory.Pointer;
import org.elece.storage.file.FileChannel;
import org.elece.storage.file.FileHandlerPool;

import java.nio.file.Path;
import java.util.Optional;

/**
 * Responsible for managing the storage of objects using a disk-based file structure.
 * The file storage is divided into chunks, with each chunk corresponding to a file on disk.
 */
public class DiskPageDatabaseStorageManager implements DatabaseStorageManager {
    private final PageBuffer pageBuffer;
    private final DbConfig dbConfig;
    private final FileHandlerPool fileHandlerPool;
    private final ReservedSlotTracer reservedSlotTracer;

    public DiskPageDatabaseStorageManager(DbConfig dbConfig, FileHandlerPool fileHandlerPool,
                                          ReservedSlotTracer reservedSlotTracer) {
        this.dbConfig = dbConfig;
        this.fileHandlerPool = fileHandlerPool;
        this.reservedSlotTracer = reservedSlotTracer;
        this.pageBuffer = new PageBuffer(dbConfig, new DefaultPageFactory(dbConfig, fileHandlerPool, this::getDbFileName), fileHandlerPool, this::getDbFileName);
    }

    /**
     * Stores the given data in the database by attempting to reuse a free slot, use the last page, or create a new chunk if necessary.
     *
     * @param tableId The identifier of the table in which the data will be stored.
     * @param data    The data to be stored in the database.
     * @return A Pointer object indicating the storage location of the data.
     * @throws DbException              If there is a database-related error during the storage operation.
     * @throws StorageException         If there is an error related to storage mechanisms.
     * @throws InterruptedTaskException If the task is interrupted during the storage operation.
     * @throws FileChannelException     If there is an error with the file channel during the storage process.
     */
    @Override
    public Pointer store(int tableId, byte[] data) throws DbException, StorageException, InterruptedTaskException,
                                                          FileChannelException {
        // Try to find a free slot that can fit the data.
        Optional<DbObjectSlotLocation> optionalDbObjectSlotLocation = this.reservedSlotTracer.getFreeDbObjectSlotLocation(data.length);

        if (optionalDbObjectSlotLocation.isPresent()) {
            DbObjectSlotLocation dbObjectSlotLocation = optionalDbObjectSlotLocation.get();
            Page page = this.getBufferedPage(dbObjectSlotLocation.pointer().getChunk(), dbObjectSlotLocation.pointer().getPosition());
            int offset = (int) (dbObjectSlotLocation.pointer().getPosition() % this.dbConfig.getDbPageSize());

            Optional<DbObject> optionalDbObject = page.getDbObjectWrapper(offset);
            if (optionalDbObject.isPresent()) {
                try {
                    this.store(optionalDbObject.get(), tableId, data);
                    return dbObjectSlotLocation.pointer();
                } finally {
                    this.pageBuffer.release(PageTitle.of(page));
                }
            }
        }

        DbObject dbObject = null;
        Page page = null;

        // Try to use the last buffered page.
        Optional<Page> optionalLastPage = pageBuffer.getBufferedLastPage();
        if (optionalLastPage.isPresent()) {
            page = optionalLastPage.get();
            try {
                Optional<DbObject> optionalDbObject = page.getEmptyDbObject(data.length);
                if (optionalDbObject.isPresent()) {
                    dbObject = optionalDbObject.get();
                }
            } catch (DbException exception) {
                this.pageBuffer.release(PageTitle.of(page));
                throw exception;
            }
        }

        // Create new page if no suitable page is found.
        if (dbObject == null) {
            page = pageBuffer.getBufferedNewPage();
            try {
                Optional<DbObject> emptyDBObject = page.getEmptyDbObject(data.length);
                if (emptyDBObject.isEmpty()) {
                    throw new DbException(DbError.INVALID_DATABASE_OBJECT_ERROR, "Not possible to create object in newly created page");
                }
                dbObject = emptyDBObject.get();
            } catch (DbException exception) {
                this.pageBuffer.release(PageTitle.of(page));
                throw exception;
            }
        }

        // Store the data in the DbObject and return the new pointer.
        try {
            this.store(dbObject, tableId, data);
            return new Pointer(Pointer.TYPE_DATA, ((long) page.getPageNumber() * this.dbConfig.getDbPageSize()) + dbObject.getBegin(), page.getChunk());
        } finally {
            this.pageBuffer.release(PageTitle.of(page));
        }
    }

    /**
     * Updates the data of an existing database object at the specified location, identified by a pointer.
     * This method acquires a page from the page buffer, locates the database object within the page,
     * verifies the object's existence and if the object is still alive. If so, we modify the object's data,
     * and commits the changes before releasing the page back to the buffer.
     *
     * @param pointer A Pointer object that specifies the chunk and position on the page to locate the database object.
     * @param newData A byte array containing the new data to update the database object with.
     * @throws DbException              If an invalid database object is found at the pointer location, or if the object is not active.
     * @throws StorageException         If there is an error related to storage mechanisms.
     * @throws InterruptedTaskException If the task is interrupted during the update process.
     * @throws FileChannelException     If there is an error with the file channel during the update process.
     */
    @Override
    public void update(Pointer pointer, byte[] newData) throws DbException, StorageException, InterruptedTaskException,
                                                               FileChannelException {
        PageTitle pageTitle = new PageTitle(pointer.getChunk(), (int) (pointer.getPosition() / this.dbConfig.getDbPageSize()));
        Page page = this.pageBuffer.acquire(pageTitle);

        try {
            Optional<DbObject> optionalDbObject = page.getDbObjectWrapper((int) (pointer.getPosition() % this.dbConfig.getDbPageSize()));

            if (optionalDbObject.isEmpty()) {
                throw new DbException(DbError.INVALID_DATABASE_OBJECT_ERROR, "No object found in the pointer location");
            }

            DbObject dbObject = optionalDbObject.get();

            if (!dbObject.isAlive()) {
                throw new DbException(DbError.INVALID_DATABASE_OBJECT_ERROR, "Object is not active");
            }

            dbObject.modifyData(newData);

            this.commitPage(dbObject.getPage());
        } finally {
            this.pageBuffer.release(pageTitle);
        }
    }

    /**
     * Selects a database object from a storage location specified by a pointer and reads it from the disk.
     * If the object is not alive, it is considered non-existent.
     *
     * @param pointer A Pointer object that specifies the location of the desired
     *                database object, including the chunk and position on the page.
     * @return An Optional containing the DbObject if it exists at the specified location, or an empty
     */
    @Override
    public Optional<DbObject> select(Pointer pointer) throws DbException, InterruptedTaskException, StorageException,
                                                             FileChannelException {
        PageTitle pageTitle = new PageTitle(pointer.getChunk(), (int) (pointer.getPosition() / this.dbConfig.getDbPageSize()));
        Page page = this.pageBuffer.acquire(pageTitle);

        try {
            return page.getDbObjectWrapper((int) (pointer.getPosition() % this.dbConfig.getDbPageSize()));
        } finally {
            this.pageBuffer.release(pageTitle);
        }
    }

    /**
     * Marks a database object for removal by saving its position and length for reuse by future objects.
     * This also marks the object as no longer active.
     *
     * @param pointer A Pointer object that specifies the location of the database object to remove,
     *                including the chunk and position on the page.
     * @throws DbException              If a database-related error occurs during the removal operation.
     * @throws StorageException         If an error related to storage mechanisms occurs.
     * @throws InterruptedTaskException If the task is interrupted during the removal operation.
     * @throws FileChannelException     If there is an error with the file channel during the removal process.
     */
    @Override
    public void remove(Pointer pointer) throws DbException, StorageException, InterruptedTaskException,
                                               FileChannelException {
        PageTitle pageTitle = new PageTitle(pointer.getChunk(), (int) (pointer.getPosition() / this.dbConfig.getDbPageSize()));
        Page page = this.pageBuffer.acquire(pageTitle);

        try {
            Optional<DbObject> optionalDbObject = page.getDbObjectWrapper((int) (pointer.getPosition() % this.dbConfig.getDbPageSize()));
            if (optionalDbObject.isPresent()) {
                DbObject dbObject = optionalDbObject.get();
                dbObject.deactivate();
                this.reservedSlotTracer.add(new DbObjectSlotLocation(pointer, dbObject.getLength()));
                this.commitPage(page);
            }
        } finally {
            this.pageBuffer.release(pageTitle);
        }
    }

    private Page getBufferedPage(int chunk, long offset) throws DbException, InterruptedTaskException,
                                                                FileChannelException, StorageException {
        int pageNumber = (int) (offset / this.dbConfig.getDbPageSize());
        PageTitle pageTitle = new PageTitle(chunk, pageNumber);
        return this.pageBuffer.acquire(pageTitle);
    }

    private Path getDbFileName(int chunk) {
        return Path.of(this.dbConfig.getBaseDbPath(), String.format("elece_%d.db.bin", chunk));
    }

    private void commitPage(Page page) throws InterruptedTaskException,
                                              StorageException, FileChannelException {
        Path path = getDbFileName(page.getChunk());
        FileChannel fileChannel = this.fileHandlerPool.acquireFileHandler(path);

        fileChannel.write((long) page.getPageNumber() * this.dbConfig.getDbPageSize(), page.getData());

        this.fileHandlerPool.releaseFileHandler(path);
    }

    private void store(DbObject dbObject, int tableId, byte[] data) throws DbException, InterruptedTaskException,
                                                                           StorageException, FileChannelException {
        dbObject.activate();
        dbObject.modifyData(data);
        dbObject.setTableId(tableId);
        dbObject.setSize(data.length);
        this.commitPage(dbObject.getPage());
    }
}
