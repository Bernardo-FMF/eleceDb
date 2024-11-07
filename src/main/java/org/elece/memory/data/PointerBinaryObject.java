package org.elece.memory.data;

import org.elece.exception.BTreeException;
import org.elece.memory.Pointer;

public class PointerBinaryObject extends AbstractBinaryObject<Pointer> {
    public static int BYTES = Pointer.BYTES;

    public PointerBinaryObject(byte[] bytes) {
        super(bytes);
    }

    @Override
    public Pointer asObject() {
        return Pointer.fromBytes(bytes);
    }

    @Override
    public boolean hasValue() {
        return bytes[0] != 0x00;
    }

    @Override
    public int size() {
        return BYTES;
    }

    public static class Factory implements BinaryObjectFactory<Pointer> {
        @Override
        public PointerBinaryObject create(Pointer pointer) throws BTreeException {
            return new PointerBinaryObject(pointer.toBytes());
        }

        @Override
        public PointerBinaryObject create(byte[] bytes, int beginning) {
            byte[] value = new byte[BYTES];
            System.arraycopy(bytes, beginning, value, 0, Pointer.BYTES);
            return new PointerBinaryObject(value);
        }

        @Override
        public int size() {
            return BYTES;
        }
    }
}
