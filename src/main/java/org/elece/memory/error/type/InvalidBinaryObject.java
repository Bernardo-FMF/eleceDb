package org.elece.memory.error.type;

import org.elece.memory.tree.node.data.IBinaryObject;

public class InvalidBinaryObject implements IBTreeError {
    private final Object obj;
    private final Class<? extends IBinaryObject<?>> objClass;

    public InvalidBinaryObject(Object obj, Class<? extends IBinaryObject<?>> objClass) {
        this.obj = obj;
        this.objClass = objClass;
    }

    @Override
    public String format() {
        return format("Internal tree error", String.format("Binary object %s is not valid for type %s", obj.toString(), objClass));
    }
}
