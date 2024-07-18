package org.elece.sql.token;

import org.elece.sql.token.chain.IProcessorChain;
import org.elece.sql.token.chain.ProcessorChain;
import org.elece.sql.token.error.TokenizerException;
import org.elece.sql.token.model.SymbolToken;
import org.elece.sql.token.model.Token;
import org.elece.sql.token.model.type.Symbol;
import org.elece.sql.token.processor.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class Tokenizer implements ITokenizer {
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

                if (token.getToken().getTokenType() == Token.TokenType.SymbolToken && ((SymbolToken)token.getToken()).getSymbol() == Symbol.Eof) {
                    reachedEof.set(true);
                }
            }
        }

        return tokens;
    }

    private TokenWrapper nextToken() {
        return tokenProcessors.process(stream);
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
