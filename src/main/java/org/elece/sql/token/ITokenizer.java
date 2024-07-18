package org.elece.sql.token;

import org.elece.sql.token.error.TokenizerException;

import java.util.List;

public interface ITokenizer {
    List<TokenWrapper> tokenize() throws TokenizerException;
}
