package org.elece.sql.token;

import org.elece.sql.token.error.TokenizerException;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class Tokenizer implements ITokenizer {
    private final CharStream stream;
    private final AtomicBoolean reachedEof;

    public Tokenizer(String input) {
        this.stream = new CharStream(input);
        this.reachedEof = new AtomicBoolean(false);
    }

    @Override
    public List<TokenWrapper> tokenize() throws TokenizerException {
        List<TokenWrapper> tokens = new LinkedList<>();
        for (TokenWrapper token : this.iterable()) {
            if (token.hasError()) {
                throw new TokenizerException(token.getError());
            }

            if (token.hasToken()) {
                tokens.add(token);
            }
        }

        return tokens;
    }

    private TokenWrapper nextToken() {
        //TODO
        return null;
    }

    private Iterable<TokenWrapper> iterable() {
        return () -> new TokenIterable(this);
    }

    private record TokenIterable(Tokenizer tokenizer) implements Iterator<TokenWrapper> {
        @Override
        public boolean hasNext() {
            return !this.tokenizer.reachedEof.get();
        }

        @Override
        public TokenWrapper next() {
            return this.tokenizer.nextToken();
        }
    }
}
