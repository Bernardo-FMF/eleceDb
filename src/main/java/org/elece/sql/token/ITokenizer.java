package org.elece.sql.token;

import org.elece.sql.error.TokenizerException;

public interface ITokenizer {
    IPeekableIterator<TokenWrapper> tokenize() throws TokenizerException;
}
