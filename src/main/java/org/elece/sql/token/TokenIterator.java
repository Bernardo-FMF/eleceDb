package org.elece.sql.token;

import java.util.Iterator;
import java.util.function.Predicate;

public class TokenIterator<T> implements PeekableIterator<T> {
    private final Iterator<T> iterator;

    public TokenIterator(Iterator<T> iterator) {
        this.iterator = iterator;
    }

    private boolean peeked = false;
    private T peekedValue = null;

    @Override
    public boolean hasNext() {
        return iterator.hasNext() || peeked;
    }

    @Override
    public T next() {
        T value;
        if (peeked) {
            peeked = false;
            value = peekedValue;
        } else if (iterator.hasNext()) {
            value = iterator.next();
        } else {
            value = null;
        }
        return value;
    }

    @Override
    public T peek() {
        T value;
        if (!peeked) {
            peeked = true;
            if (iterator.hasNext()) {
                peekedValue = iterator.next();
            } else {
                peekedValue = null;
                peeked = false;
            }
        }
        value = peekedValue;
        return value;
    }

    @Override
    public Iterator<T> takeWhile(Predicate<T> predicate) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return TokenIterator.this.hasNext() && predicate.test(peek());
            }

            @Override
            public T next() {
                T target = TokenIterator.this.next();
                if (predicate.test(target) && TokenIterator.this.hasNext()) {
                    target = next();
                }

                return target;
            }
        };
    }

}
