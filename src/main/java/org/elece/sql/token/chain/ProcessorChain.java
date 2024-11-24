package org.elece.sql.token.chain;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.processor.TokenProcessor;

import java.util.Objects;

public class ProcessorChain {
    private ProcessorChain nextChain;
    private final TokenProcessor<Character> processor;

    public ProcessorChain(TokenProcessor<Character> processor) {
        this.processor = processor;
        this.nextChain = null;
    }

    public void setNextChain(ProcessorChain nextChain) {
        this.nextChain = nextChain;
    }

    public TokenWrapper process(CharStream stream) {
        if (processor.matches(stream.peek())) {
            return processor.consume(stream);
        } else {
            if (!Objects.isNull(nextChain)) {
                return nextChain.process(stream);
            }
        }
        return TokenWrapper.builder().build();
    }
}
