package org.elece.memory.tree.node.data;

import org.elece.exception.btree.BTreeException;
import org.elece.exception.btree.type.InvalidBinaryObjectError;
import org.elece.utils.BinaryUtils;

public class IntegerBinaryObject extends AbstractBinaryObject<Integer> {
    public static int BYTES = Integer.BYTES;

    public IntegerBinaryObject() {
    }

    public IntegerBinaryObject(byte[] bytes) {
        super(bytes);
    }

    @Override
    public BinaryObject<Integer> load(Integer integer) throws BTreeException {
        // TODO: add logic in analyzer to avoid this type of situations
        if (integer == 0) {
            throw new BTreeException(new InvalidBinaryObjectError(integer, this.getClass()));
        }
        return new IntegerBinaryObject(BinaryUtils.integerToBytes(integer));
    }

    @Override
    public BinaryObject<Integer> load(byte[] bytes, int beginning) {
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
}
