package org.elece.sql.token;

import java.util.Iterator;
import java.util.function.Predicate;

public interface PeekableIterator<T> extends Iterator<T> {
    T peek();
    Iterator<T> takeWhile(Predicate<T> predicate);
}
