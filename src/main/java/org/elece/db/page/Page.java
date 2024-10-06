package org.elece.db.page;

import org.elece.db.DbObject;
import org.elece.exception.db.DbException;
import org.elece.utils.BinaryUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Page {
    public static int META_BYTES = Integer.BYTES;

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

    public synchronized Optional<DbObject> getDBObjectWrapper(int offset) throws DbException {
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

    public synchronized Optional<DbObject> getEmptyDBObject(int length) throws DbException {
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
