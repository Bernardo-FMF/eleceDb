package org.elece.sql.token;

import org.elece.exception.sql.TokenizerException;

public interface ITokenizer {
    IPeekableIterator<TokenWrapper> tokenize() throws TokenizerException;
}
