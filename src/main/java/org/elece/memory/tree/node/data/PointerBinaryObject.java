package org.elece.memory.tree.node.data;

import org.elece.memory.Pointer;

public class PointerBinaryObject extends AbstractBinaryObject<Pointer> {
    public static int BYTES = Pointer.BYTES;

    public PointerBinaryObject() {
    }

    public PointerBinaryObject(byte[] bytes) {
        super(bytes);
    }

    @Override
    public PointerBinaryObject load(Pointer pointer) {
        return new PointerBinaryObject(pointer.toBytes());
    }

    @Override
    public PointerBinaryObject load(byte[] bytes, int beginning) {
        byte[] value = new byte[BYTES];
        System.arraycopy(bytes, beginning, value, 0, Pointer.BYTES);
        return new PointerBinaryObject(value);
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

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
