package org.elece.exception.btree.type;

import org.elece.exception.DbError;
import org.elece.memory.tree.node.data.BinaryObject;

public class InvalidBinaryObject implements DbError {
    private final Object obj;
    private final Class<? extends BinaryObject<?>> objClass;

    public InvalidBinaryObject(Object obj, Class<? extends BinaryObject<?>> objClass) {
        this.obj = obj;
        this.objClass = objClass;
    }

    @Override
    public String format() {
        return String.format("Binary object %s is not valid for type %s", obj.toString(), objClass);
    }
}
