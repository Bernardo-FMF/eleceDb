package org.elece.db;

import org.elece.memory.Pointer;

public record DbObjectSlotLocation(Pointer pointer, int length) {
}