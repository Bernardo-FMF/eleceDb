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

    @Override
    public Pointer store(int tableId, byte[] data) throws DbException, StorageException, InterruptedTaskException,
                                                          FileChannelException {
        // Try to find a free slot that can fit the data.
        Optional<DbObjectSlotLocation> optionalDbObjectSlotLocation = this.reservedSlotTracer.getFreeDbObjectSlotLocation(data.length);

        if (optionalDbObjectSlotLocation.isPresent()) {
            DbObjectSlotLocation dbObjectSlotLocation = optionalDbObjectSlotLocation.get();
            Page page = this.getBufferedPage(dbObjectSlotLocation.pointer().getChunk(), dbObjectSlotLocation.pointer().getPosition());
            int offset = (int) (dbObjectSlotLocation.pointer().getPosition() % this.dbConfig.getDbPageSize());

            Optional<DbObject> optionalDbObject = page.getDBObjectWrapper(offset);
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
                Optional<DbObject> optionalDbObject = page.getEmptyDBObject(data.length);
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
                Optional<DbObject> emptyDBObject = page.getEmptyDBObject(data.length);
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

    @Override
    public void update(Pointer pointer, byte[] newData) throws DbException, StorageException, InterruptedTaskException,
                                                               FileChannelException {
        PageTitle pageTitle = new PageTitle(pointer.getChunk(), (int) (pointer.getPosition() / this.dbConfig.getDbPageSize()));
        Page page = this.pageBuffer.acquire(pageTitle);

        try {
            Optional<DbObject> optionalDbObject = page.getDBObjectWrapper((int) (pointer.getPosition() % this.dbConfig.getDbPageSize()));

            if (optionalDbObject.isEmpty()) {
                throw new DbException(DbError.INVALID_DATABASE_OBJECT_ERROR, "No object found in the pointer location");
            }

            DbObject dbObject = optionalDbObject.get();
            dbObject.modifyData(newData);

            this.commitPage(dbObject.getPage());
        } finally {
            this.pageBuffer.release(pageTitle);
        }
    }

    /**
     * Reads a DbObject from the database storage using a pointer.
     * This pointer is used to obtain the file the object is in using the chunk, and the position is used to determine the offset
     * of the page where the object is present.
     *
     * @param pointer A Pointer object that indicates the chunk and position within the page.
     * @return An Optional containing the selected DbObject if found, or an empty Optional if not found.
     */
    @Override
    public Optional<DbObject> select(Pointer pointer) throws DbException, InterruptedTaskException, StorageException,
                                                             FileChannelException {
        PageTitle pageTitle = new PageTitle(pointer.getChunk(), (int) (pointer.getPosition() / this.dbConfig.getDbPageSize()));
        Page page = this.pageBuffer.acquire(pageTitle);

        try {
            return page.getDBObjectWrapper((int) (pointer.getPosition() % this.dbConfig.getDbPageSize()));
        } finally {
            this.pageBuffer.release(pageTitle);
        }
    }

    @Override
    public void remove(Pointer pointer) throws DbException, StorageException, InterruptedTaskException,
                                               FileChannelException {
        PageTitle pageTitle = new PageTitle(pointer.getChunk(), (int) (pointer.getPosition() / this.dbConfig.getDbPageSize()));
        Page page = this.pageBuffer.acquire(pageTitle);

        try {
            Optional<DbObject> optionalDbObject = page.getDBObjectWrapper((int) (pointer.getPosition() % this.dbConfig.getDbPageSize()));
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
