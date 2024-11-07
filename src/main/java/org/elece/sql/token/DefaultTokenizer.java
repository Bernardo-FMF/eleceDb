package org.elece.sql.token;

import org.elece.sql.token.chain.IProcessorChain;
import org.elece.sql.token.chain.ProcessorChain;
import org.elece.sql.token.model.SymbolToken;
import org.elece.sql.token.model.Token;
import org.elece.sql.token.model.type.Symbol;
import org.elece.sql.token.processor.*;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultTokenizer implements Tokenizer {
    private final CharStream stream;
    private final AtomicBoolean reachedEof;

    private static final IProcessorChain tokenProcessors;

    static {
        ProcessorChain eofChain = new ProcessorChain(new EofTokenProcessor());
        ProcessorChain whitespaceChain = new ProcessorChain(new WhitespaceTokenProcessor());
        ProcessorChain symbolChain = new ProcessorChain(new SymbolTokenProcessor());
        ProcessorChain stringChain = new ProcessorChain(new StringTokenProcessor());
        ProcessorChain numberChain = new ProcessorChain(new NumberTokenProcessor());
        ProcessorChain keywordOrIdentifierChain = new ProcessorChain(new KeywordOrIdentifierTokenProcessor());
        ProcessorChain wildcardChain = new ProcessorChain(new WildcardProcessor());

        eofChain.setNextChain(whitespaceChain);
        whitespaceChain.setNextChain(symbolChain);
        symbolChain.setNextChain(stringChain);
        stringChain.setNextChain(numberChain);
        numberChain.setNextChain(keywordOrIdentifierChain);
        keywordOrIdentifierChain.setNextChain(wildcardChain);

        tokenProcessors = eofChain;
    }

    public DefaultTokenizer(String input) {
        this.stream = new CharStream(input);
        this.reachedEof = new AtomicBoolean(false);
    }


    @Override
    public PeekableIterator<TokenWrapper> tokenize() {
        return new TokenIterator<>(new TokenIterable(this));
    }

    private TokenWrapper nextToken() {
        return tokenProcessors.process(stream);
    }

    private record TokenIterable(DefaultTokenizer defaultTokenizer) implements Iterator<TokenWrapper> {
        @Override
        public boolean hasNext() {
            return !this.defaultTokenizer.reachedEof.get();
        }

        @Override
        public TokenWrapper next() {
            TokenWrapper tokenWrapper = this.defaultTokenizer.nextToken();
            if (tokenWrapper.hasToken()) {
                if (tokenWrapper.getToken().getTokenType() == Token.TokenType.SymbolToken && ((SymbolToken) tokenWrapper.getToken()).getSymbol() == Symbol.Eof) {
                    this.defaultTokenizer.reachedEof.set(true);
                }
            }
            return tokenWrapper;
        }
    }
}
