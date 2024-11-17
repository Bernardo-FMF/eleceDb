package org.elece.query.plan.step.deserializer;

import org.elece.exception.DeserializationException;

public abstract class DeserializerStep {
    public abstract String deserialize(byte[] data) throws DeserializationException;
}
