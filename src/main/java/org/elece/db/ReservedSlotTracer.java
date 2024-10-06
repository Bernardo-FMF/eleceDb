package org.elece.db;

import java.util.Optional;

public interface ReservedSlotTracer {
    void add(DbObjectSlotLocation dbObjectSlotLocation);

    Optional<DbObjectSlotLocation> getFreeDbObjectSlotLocation(int length);
}
