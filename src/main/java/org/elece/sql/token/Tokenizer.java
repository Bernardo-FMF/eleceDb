package org.elece.sql.token;

import org.elece.exception.TokenizerException;

public interface Tokenizer {
    PeekableIterator<TokenWrapper> tokenize() throws TokenizerException;
}
