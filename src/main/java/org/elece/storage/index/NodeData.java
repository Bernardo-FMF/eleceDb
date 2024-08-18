package org.elece.storage.index;

import org.elece.memory.Pointer;

public record NodeData(Pointer pointer, byte[] bytes) {
}