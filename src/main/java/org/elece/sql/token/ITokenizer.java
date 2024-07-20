package org.elece.sql.token;

import org.elece.sql.token.error.TokenizerException;

import java.util.Iterator;

public interface ITokenizer {
    Iterator<TokenWrapper> tokenize() throws TokenizerException;
}
