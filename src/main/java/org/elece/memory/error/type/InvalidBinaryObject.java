package org.elece.memory.error.type;

import org.elece.memory.tree.node.data.BinaryObject;

public class InvalidBinaryObject implements BTreeError {
    private final Object obj;
    private final Class<? extends BinaryObject<?>> objClass;

    public InvalidBinaryObject(Object obj, Class<? extends BinaryObject<?>> objClass) {
        this.obj = obj;
        this.objClass = objClass;
    }

    @Override
    public String format() {
        return format("Internal tree error", String.format("Binary object %s is not valid for type %s", obj.toString(), objClass));
    }
}
