package org.elece.memory.tree.node.data;

import org.elece.exception.btree.BTreeException;
import org.elece.exception.btree.type.InvalidBinaryObjectError;
import org.elece.utils.BinaryUtils;

import java.nio.charset.StandardCharsets;

public class StringBinaryObject extends AbstractBinaryObject<String> {
    public final int BYTES;
    private final byte[] bytes;

    public StringBinaryObject(byte[] bytes) {
        this.bytes = bytes;
        this.BYTES = bytes.length;
    }

    @Override
    public String asObject() {
        int len = 0;
        while (len < bytes.length && bytes[len] != 0) {
            len++;
        }
        return new String(bytes, 0, len, StandardCharsets.UTF_8);
    }

    @Override
    public boolean hasValue() {
        for (byte aByte : this.bytes) {
            if (aByte != 0x00) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int size() {
        return BYTES;
    }

    public static class Factory implements BinaryObjectFactory<String> {
        private final int size;

        public Factory(int size) {
            this.size = size;
        }

        @Override
        public StringBinaryObject create(String value) throws BTreeException {
            byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
            if (valueBytes.length > size) {
                throw new BTreeException(new InvalidBinaryObjectError(value, IntegerBinaryObject.class));
            }

            byte[] data = new byte[size];

            System.arraycopy(valueBytes, 0, data, 0, valueBytes.length);

            BinaryUtils.fillPadding(valueBytes.length, size, data);

            return new StringBinaryObject(data);
        }

        @Override
        public StringBinaryObject create(byte[] bytes, int beginning) {
            byte[] data = new byte[size];

            System.arraycopy(bytes, beginning, data, 0, this.size());

            return new StringBinaryObject(data);
        }

        @Override
        public int size() {
            return size;
        }
    }
}
