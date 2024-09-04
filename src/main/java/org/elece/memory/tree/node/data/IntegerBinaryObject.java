package org.elece.memory.tree.node.data;

import org.elece.exception.btree.BTreeException;
import org.elece.exception.btree.type.InvalidBinaryObjectError;
import org.elece.utils.BinaryUtils;

public class IntegerBinaryObject extends AbstractBinaryObject<Integer> {
    public static int BYTES = Integer.BYTES;

    public IntegerBinaryObject(byte[] bytes) {
        super(bytes);
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

    public static class Factory implements BinaryObjectFactory<Integer> {
        @Override
        public IntegerBinaryObject create(Integer value) throws BTreeException {
            if (value == 0) {
                throw new BTreeException(new InvalidBinaryObjectError(value, IntegerBinaryObject.class));
            }
            return new IntegerBinaryObject(BinaryUtils.integerToBytes(value));
        }

        @Override
        public IntegerBinaryObject create(byte[] bytes, int beginning) {
            byte[] value = new byte[BYTES];
            System.arraycopy(bytes, beginning, value, 0, Integer.BYTES);
            return new IntegerBinaryObject(value);
        }

        @Override
        public int size() {
            return BYTES;
        }
    }
}
