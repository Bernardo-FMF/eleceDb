package org.elece.sql.token.chain;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.TokenWrapper;

public interface IProcessorChain {
    void setNextChain(IProcessorChain nextChain);
    TokenWrapper process(CharStream stream);
}