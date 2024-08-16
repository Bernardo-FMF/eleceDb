package org.elece.memory.tree.node.data;

import org.elece.memory.BinaryUtils;
import org.elece.memory.error.BTreeException;
import org.elece.memory.error.type.InvalidBinaryObject;

public class IntegerBinaryObject extends AbstractBinaryObject<Integer> {
    public static int BYTES = Integer.BYTES;

    public IntegerBinaryObject() {
    }

    public IntegerBinaryObject(byte[] bytes) {
        super(bytes);
    }

    @Override
    public IBinaryObject<Integer> load(Integer integer) throws BTreeException {
        // TODO: add logic in analyzer to avoid this type of situations
        if (integer == 0) {
            throw new BTreeException(new InvalidBinaryObject(integer, this.getClass()));
        }
        return new IntegerBinaryObject(BinaryUtils.integerToBytes(integer));
    }

    @Override
    public IBinaryObject<Integer> load(byte[] bytes, int beginning) {
        byte[] value = new byte[BYTES];
        System.arraycopy(bytes, beginning, value, 0, Integer.BYTES);
        return new IntegerBinaryObject(value);
    }

    @Override
    public Integer asObject() {
        return BinaryUtils.bytesToInteger(bytes, 0);
    }

    @Override
    public boolean hasValue() {
        return asObject() != 0;
    }

    @Override
    public int size() {
        return BYTES;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }
}
