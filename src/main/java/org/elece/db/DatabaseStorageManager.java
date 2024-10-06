package org.elece.db;

import org.elece.exception.db.DbException;
import org.elece.exception.storage.StorageException;
import org.elece.memory.Pointer;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public interface DatabaseStorageManager {
    Pointer store(int collectionId, byte[] data) throws DbException, StorageException, IOException, ExecutionException, InterruptedException;

    void update(Pointer pointer, Consumer<DbObject> dbObjectConsumer) throws DbException, StorageException, IOException, InterruptedException;

    Optional<DbObject> select(Pointer pointer) throws DbException;

    void remove(Pointer pointer) throws DbException, StorageException, IOException, InterruptedException;
}
