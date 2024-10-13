package org.elece.db;

import org.elece.db.page.Page;
import org.elece.exception.db.DbException;
import org.elece.exception.db.type.InvalidDbObjectError;
import org.elece.utils.BinaryUtils;

public class DbObject {
    public static byte ALIVE_OBJ = 0x01;
    public static int FLAG_BYTES = 1;
    public static int META_BYTES = FLAG_BYTES + (2 * Integer.BYTES);
    public static int META_COLLECTION_ID_OFFSET = FLAG_BYTES;
    public static int META_SIZE_OFFSET = FLAG_BYTES + Integer.BYTES;

    private final byte[] wrappedData;
    private final int begin;
    private final int end;
    private final int length;
    private final Page page;

    public DbObject(Page page, int begin, int end) throws DbException {
        this.wrappedData = page.getData();
        this.begin = begin;
        this.end = end;
        length = end - begin;
        this.page = page;
        this.setSize(length);
        this.verify();
    }

    public void setSize(int size) {
        System.arraycopy(BinaryUtils.integerToBytes(size), 0, this.wrappedData, begin + META_SIZE_OFFSET, Integer.BYTES);
    }

    public void deactivate() {
        this.wrappedData[begin] = (byte) (wrappedData[begin] & ~ALIVE_OBJ);
    }

    public void activate() {
        this.wrappedData[begin] = (byte) (wrappedData[begin] | ALIVE_OBJ);
    }

    private void verify() throws DbException {
        if (end > this.wrappedData.length - 1) {
            throw new DbException(new InvalidDbObjectError(String.format("Object end marker %d exceeds the data size of %d", end, wrappedData.length)));
        }

        int min = META_BYTES + 1;
        if (this.length < min) {
            throw new DbException(new InvalidDbObjectError(String.format("Object length is less than the minimum size of %d", min)));
        }
    }

    public static int getDataSize(byte[] wrappedData, int begin) {
        return BinaryUtils.bytesToInteger(wrappedData, begin + META_SIZE_OFFSET);
    }

    public byte[] getData() {
        byte[] result = new byte[getDataSize()];
        System.arraycopy(this.wrappedData, begin + META_BYTES, result, 0, result.length);
        return result;
    }

    public int getDataSize() {
        return DbObject.getDataSize(this.wrappedData, this.begin);
    }

    public static int getWrappedSize(int length) {
        return META_SIZE_OFFSET + length;
    }

    public int getLength() {
        return length;
    }

    public Page getPage() {
        return page;
    }

    public void modifyData(byte[] value) throws DbException {
        if (value.length > this.length - META_BYTES) {
            throw new DbException(new InvalidDbObjectError(String.format("Object length %d exceeds the maximum size of %d", value.length, this.length - META_BYTES)));
        }

        System.arraycopy(value, 0, this.wrappedData, begin + META_BYTES, value.length);
    }

    public void setTableId(int tableId) {
        System.arraycopy(BinaryUtils.integerToBytes(tableId), 0, this.wrappedData, begin + META_COLLECTION_ID_OFFSET, Integer.BYTES);
    }

    public long getBegin() {
        return begin;
    }

    public boolean isAlive() {
        return DbObject.isAlive(this.wrappedData, this.begin);
    }

    public static boolean isAlive(byte[] wrappedData, int begin) {
        return (wrappedData[begin] & ALIVE_OBJ) == ALIVE_OBJ;
    }

    public byte[] readData(int offset, int size) {
        byte[] output = new byte[size];

        System.arraycopy(this.wrappedData, begin + META_BYTES + offset, output, 0, size);
        return output;
    }
}
