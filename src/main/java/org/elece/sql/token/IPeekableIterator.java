package org.elece.sql.token;

import java.util.Iterator;
import java.util.function.Predicate;

public interface IPeekableIterator<T> extends Iterator<T> {
    T peek();
    Iterator<T> takeWhile(Predicate<T> predicate);
}
