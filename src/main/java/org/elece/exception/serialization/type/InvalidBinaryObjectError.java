package org.elece.exception.serialization.type;

import org.elece.exception.DbError;
import org.elece.serializer.Serializer;

public class InvalidBinaryObjectError implements DbError {
    private final Object obj;
    private final Class<? extends Serializer<?>> objClass;

    public InvalidBinaryObjectError(Object obj, Class<? extends Serializer<?>> objClass) {
        this.obj = obj;
        this.objClass = objClass;
    }

    @Override
    public String format() {
        return String.format("Binary object %s is not valid for type %s", obj.toString(), objClass);
    }
}
