package org.elece.db.page;

import org.elece.db.DbObject;
import org.elece.exception.DbException;
import org.elece.utils.BinaryUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Page {
    public static final int META_BYTES = Integer.BYTES;

    private final int pageNumber;
    private final int chunk;
    private int cursorPosition;
    private final byte[] data;
    private final Map<Integer, DbObject> objectPool;

    public Page(int pageNumber, int chunk, byte[] data) {
        this.pageNumber = pageNumber;
        this.chunk = chunk;
        this.cursorPosition = BinaryUtils.bytesToInteger(data, 0) + META_BYTES;
        this.data = data;
        this.objectPool = new HashMap<>();
    }

    public synchronized Optional<DbObject> getDbObjectWrapper(int offset) throws DbException {
        if (objectPool.containsKey(offset)) {
            return Optional.of(objectPool.get(offset));
        }

        int dataSize = DbObject.getDataSize(getData(), offset);
        if (dataSize == 0) {
            return Optional.empty();
        }

        int wrappedSize = DbObject.getWrappedSize(dataSize);
        DbObject dbObject = new DbObject(this, offset, offset + wrappedSize);

        objectPool.put(offset, dbObject);
        return Optional.of(dbObject);
    }

    /**
     * Tries to create an empty object with a given length. If the page segment has no space available to fit the object,
     * then the object can't be added to the current page.
     *
     * @param length The desired length of the DbObject to be created.
     * @return An Optional containing the newly created DbObject if space is available; otherwise, an empty Optional
     * @throws DbException If an error occurs during the creation of the DbObject
     */
    public synchronized Optional<DbObject> getEmptyDbObject(int length) throws DbException {
        if (getData().length - cursorPosition > length + DbObject.META_BYTES) {
            DbObject dbObject = new DbObject(this, cursorPosition, cursorPosition + length + DbObject.META_BYTES);
            Optional<DbObject> output = Optional.of(dbObject);
            this.objectPool.putIfAbsent(cursorPosition, dbObject);
            this.setCursorPosition(this.cursorPosition + length + DbObject.META_BYTES);
            return output;
        }
        return Optional.empty();
    }

    private void setCursorPosition(int cursorPosition) {
        this.cursorPosition = cursorPosition;
        System.arraycopy(BinaryUtils.integerToBytes(cursorPosition), 0, this.data, 0, Integer.BYTES);
    }

    public byte[] getData() {
        return data;
    }

    public int getChunk() {
        return chunk;
    }

    public int getPageNumber() {
        return pageNumber;
    }
}
