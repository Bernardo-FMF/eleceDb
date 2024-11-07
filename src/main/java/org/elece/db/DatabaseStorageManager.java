package org.elece.db;

import org.elece.exception.DbException;
import org.elece.exception.StorageException;
import org.elece.memory.Pointer;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public interface DatabaseStorageManager {
    Pointer store(int tableId, byte[] data) throws DbException, StorageException, IOException, ExecutionException, InterruptedException;

    void update(Pointer pointer, byte[] newData) throws DbException, StorageException, IOException,
                                                        InterruptedException;

    Optional<DbObject> select(Pointer pointer) throws DbException;

    void remove(Pointer pointer) throws DbException, StorageException, IOException, InterruptedException;
}
