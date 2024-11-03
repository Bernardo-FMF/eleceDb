package org.elece.query.plan.step.selector;

import org.elece.db.DbObject;

import java.util.Optional;

public abstract class SelectorStep {
    public abstract Optional<byte[]> next(DbObject dbObject);
}
