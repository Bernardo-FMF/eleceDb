package org.elece.db;

import org.elece.memory.Pointer;

import java.util.Objects;

public final class DbObjectSlotLocation {
    private final Pointer pointer;
    private final int length;

    public DbObjectSlotLocation(Pointer pointer, int length) {
        this.pointer = pointer;
        this.length = length;
    }

    public Pointer pointer() {
        return pointer;
    }

    public int length() {
        return length;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DbObjectSlotLocation dbObjectSlotLocation = (DbObjectSlotLocation) obj;
        return length == dbObjectSlotLocation.length && Objects.equals(pointer, dbObjectSlotLocation.pointer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pointer, length);
    }
}