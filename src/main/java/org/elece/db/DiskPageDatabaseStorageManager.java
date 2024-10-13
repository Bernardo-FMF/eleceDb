package org.elece.db;

import org.elece.config.DbConfig;
import org.elece.db.page.DefaultPageFactory;
import org.elece.db.page.Page;
import org.elece.db.page.PageBuffer;
import org.elece.db.page.PageTitle;
import org.elece.exception.RuntimeDbException;
import org.elece.exception.db.DbException;
import org.elece.exception.db.type.InvalidDbObjectError;
import org.elece.exception.storage.StorageException;
import org.elece.memory.Pointer;
import org.elece.storage.file.FileHandlerPool;
import org.elece.utils.FileUtils;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class DiskPageDatabaseStorageManager implements DatabaseStorageManager {
    private final PageBuffer pageBuffer;
    private final DbConfig dbConfig;
    private final FileHandlerPool fileHandlerPool;
    private final ReservedSlotTracer reservedSlotTracer;

    public DiskPageDatabaseStorageManager(DbConfig dbConfig, FileHandlerPool fileHandlerPool, ReservedSlotTracer reservedSlotTracer) {
        this.dbConfig = dbConfig;
        this.fileHandlerPool = fileHandlerPool;
        this.reservedSlotTracer = reservedSlotTracer;
        this.pageBuffer = new PageBuffer(dbConfig, new DefaultPageFactory(dbConfig, fileHandlerPool, this::getDBFileName), fileHandlerPool, this::getDBFileName);
    }

    @Override
    public Pointer store(int tableId, byte[] data) throws DbException, StorageException, IOException, ExecutionException, InterruptedException {
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
                    throw new DbException(new InvalidDbObjectError("Not possible to create object in newly created page"));
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
    public void update(Pointer pointer, Consumer<DbObject> dbObjectConsumer) throws DbException, StorageException, IOException, InterruptedException {
        PageTitle pageTitle = new PageTitle(pointer.getChunk(), (int) (pointer.getPosition() / this.dbConfig.getDbPageSize()));
        Page page = this.pageBuffer.acquire(pageTitle);

        try {
            Optional<DbObject> optionalDbObject = page.getDBObjectWrapper((int) (pointer.getPosition() % this.dbConfig.getDbPageSize()));

            if (optionalDbObject.isEmpty()) {
                throw new DbException(new InvalidDbObjectError("No object found in the pointer location"));
            }

            DbObject dbObject = optionalDbObject.get();
            dbObjectConsumer.accept(dbObject);

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
    public Optional<DbObject> select(Pointer pointer) throws DbException {
        PageTitle pageTitle = new PageTitle(pointer.getChunk(), (int) (pointer.getPosition() / this.dbConfig.getDbPageSize()));
        Page page = this.pageBuffer.acquire(pageTitle);

        try {
            return page.getDBObjectWrapper((int) (pointer.getPosition() % this.dbConfig.getDbPageSize()));
        } catch (DbException exception) {
            throw new RuntimeDbException(exception.getDbError());
        } finally {
            this.pageBuffer.release(pageTitle);
        }
    }

    @Override
    public void remove(Pointer pointer) throws DbException, StorageException, IOException, InterruptedException {
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

    private Page getBufferedPage(int chunk, long offset) throws DbException {
        int pageNumber = (int) (offset / this.dbConfig.getDbPageSize());
        PageTitle pageTitle = new PageTitle(chunk, pageNumber);
        return this.pageBuffer.acquire(pageTitle);
    }

    private Path getDBFileName(int chunk) {
        return Path.of(this.dbConfig.getBaseDbPath(), String.format("elece_%d.db.bin", chunk));
    }

    private void commitPage(Page page) throws IOException, InterruptedException {
        Path path = getDBFileName(page.getChunk());
        AsynchronousFileChannel fileChannel = this.fileHandlerPool.acquireFileHandler(path);

        try {
            FileUtils.write(fileChannel, (long) page.getPageNumber() * this.dbConfig.getDbPageSize(), page.getData()).get();
        } catch (InterruptedException | ExecutionException exception) {
            throw new RuntimeException(exception);
        }

        this.fileHandlerPool.releaseFileHandler(path);
    }

    private void store(DbObject dbObject, int tableId, byte[] data) throws DbException, IOException, InterruptedException {
        dbObject.activate();
        dbObject.modifyData(data);
        dbObject.setTableId(tableId);
        dbObject.setSize(data.length);
        this.commitPage(dbObject.getPage());
    }
}
