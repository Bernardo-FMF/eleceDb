package org.elece.utils;

import org.elece.memory.Pointer;

public class BTreeUtils {
    public static int calculateBPlusTreeSize(int degree, int keySize, int valueSize) {
        int value = 1 + (degree * (keySize + valueSize)) + (2 * Pointer.BYTES);
        int i = value % 8;
        if (i == 0) {
            return value;
        }
        return value + 8 - i;
    }
}
