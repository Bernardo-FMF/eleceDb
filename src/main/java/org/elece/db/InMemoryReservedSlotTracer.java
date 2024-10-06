package org.elece.db;

import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryReservedSlotTracer implements ReservedSlotTracer {
    public Map<Integer, Queue<DbObjectSlotLocation>> freeDbObjectSlotLocations = new ConcurrentHashMap<>();

    @Override
    public void add(DbObjectSlotLocation dbObjectSlotLocation) {
        freeDbObjectSlotLocations
                .computeIfAbsent(dbObjectSlotLocation.length(), _ -> new LinkedList<>())
                .add(dbObjectSlotLocation);
    }

    @Override
    public Optional<DbObjectSlotLocation> getFreeDbObjectSlotLocation(int length) {
        Queue<DbObjectSlotLocation> queue = freeDbObjectSlotLocations.get(length);
        if (queue == null || queue.isEmpty()) {
            return Optional.empty();
        }

        DbObjectSlotLocation dbObjectSlotLocation = queue.poll();
        if (queue.isEmpty()) {
            freeDbObjectSlotLocations.remove(length);
        }

        return Optional.of(dbObjectSlotLocation);
    }
}
