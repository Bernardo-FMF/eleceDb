package org.elece.db;

import org.elece.exception.DbException;
import org.elece.exception.FileChannelException;
import org.elece.exception.InterruptedTaskException;
import org.elece.exception.StorageException;
import org.elece.memory.Pointer;

import java.util.Optional;

public interface DatabaseStorageManager {
    Pointer store(int tableId, byte[] data) throws DbException, StorageException, InterruptedTaskException,
                                                   FileChannelException;

    void update(Pointer pointer, byte[] newData) throws DbException, StorageException, InterruptedTaskException,
                                                        FileChannelException;

    Optional<DbObject> select(Pointer pointer) throws DbException, InterruptedTaskException, StorageException,
                                                      FileChannelException;

    void remove(Pointer pointer) throws DbException, StorageException, InterruptedTaskException, FileChannelException;
}
