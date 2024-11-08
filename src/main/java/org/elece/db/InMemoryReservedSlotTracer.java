package org.elece.db;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryReservedSlotTracer implements ReservedSlotTracer {
    private final Map<Integer, Queue<DbObjectSlotLocation>> freeDbObjectSlotLocations = new ConcurrentHashMap<>();

    @Override
    public void add(DbObjectSlotLocation dbObjectSlotLocation) {
        freeDbObjectSlotLocations
                .computeIfAbsent(dbObjectSlotLocation.length(), _ -> new LinkedList<>())
                .add(dbObjectSlotLocation);
    }

    @Override
    public Optional<DbObjectSlotLocation> getFreeDbObjectSlotLocation(int length) {
        Queue<DbObjectSlotLocation> queue = freeDbObjectSlotLocations.get(length);
        if (!Objects.isNull(queue) && !queue.isEmpty()) {
            DbObjectSlotLocation dbObjectSlotLocation = queue.poll();
            if (queue.isEmpty()) {
                freeDbObjectSlotLocations.remove(length);
            }

            return Optional.of(dbObjectSlotLocation);
        }

        Integer smallestFittingLength = null;
        DbObjectSlotLocation dbObjectSlotLocation = null;

        for (Map.Entry<Integer, Queue<DbObjectSlotLocation>> entry : freeDbObjectSlotLocations.entrySet()) {
            int currentLength = entry.getKey();
            if (currentLength >= length && (smallestFittingLength == null || currentLength < smallestFittingLength)) {
                    smallestFittingLength = currentLength;
                }
        }

        if (smallestFittingLength != null) {
            Queue<DbObjectSlotLocation> smallestQueue = freeDbObjectSlotLocations.get(smallestFittingLength);
            if (smallestQueue != null && !smallestQueue.isEmpty()) {
                dbObjectSlotLocation = smallestQueue.poll();
                if (smallestQueue.isEmpty()) {
                    freeDbObjectSlotLocations.remove(smallestFittingLength);
                }
            }
        }

        return Optional.ofNullable(dbObjectSlotLocation);
    }
}
