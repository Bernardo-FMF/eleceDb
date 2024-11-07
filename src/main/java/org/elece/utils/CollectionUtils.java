package org.elece.utils;

import org.elece.exception.BTreeException;
import org.elece.exception.DbError;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class CollectionUtils {
    private CollectionUtils() {
        // private constructor
    }

    public static <T extends Comparable<T>> int findIndex(List<? extends T> list, T item) throws BTreeException {
        int index = Collections.binarySearch(list, item);
        if (index >= 0) {
            throw new BTreeException(DbError.DUPLICATE_INDEX_INSERTION_ERROR, String.format("Indexed key '%s' already exists", item));
        }

        return (index * -1) - 1;
    }

    public static <T> List<T> immutableList(Iterator<T> iterator) {
        if (!iterator.hasNext()) {
            return List.of();
        } else {
            List<T> list = new ArrayList<>();
            while (iterator.hasNext()) {
                list.add(iterator.next());
            }
            return List.copyOf(list);
        }
    }
}
