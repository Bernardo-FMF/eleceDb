package org.elece.sql.token;

import org.elece.sql.token.model.type.Whitespace;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

public class CharStream implements Iterator<Character> {
    private final String input;
    private final Location location;
    private final Iterator<Character> chars;

    public CharStream(String input) {
        this.input = input;
        this.location = new Location();
        this.chars = input.codePoints().mapToObj(c -> (char) c).iterator();
    }

    @Override
    public boolean hasNext() {
        return chars.hasNext();
    }

    @Override
    public Character next() {
        Character next = chars.next();
        if (next.equals(Whitespace.NewLine.getWhitespaceValue()[0])) {
            location.incrementLine();
            location.resetColumn();
        } else {
            location.incrementColumn();
        }
        location.incrementPointer();
        return next;
    }

    public Character peek() {
        if (!hasNext()) {
            return null;
        }
        return input.charAt(location.getPointer());
    }

    public Iterable<Character> takeWhile(Predicate<Character> predicate) {
        return () -> new Iterator<>() {
            @Override
            public boolean hasNext() {
                return CharStream.this.hasNext() && predicate.test(peek());
            }

            @Override
            public Character next() {
                return CharStream.this.next();
            }
        };
    }
}
