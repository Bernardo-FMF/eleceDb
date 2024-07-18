package org.elece.sql.token.chain;

import org.elece.sql.token.CharStream;
import org.elece.sql.token.TokenWrapper;
import org.elece.sql.token.processor.ITokenProcessor;

import java.util.Objects;

public class ProcessorChain implements IProcessorChain {
    private IProcessorChain nextChain;
    private final ITokenProcessor<Character> processor;

    public ProcessorChain(ITokenProcessor<Character> processor) {
        this.processor = processor;
        this.nextChain = null;
    }

    @Override
    public void setNextChain(IProcessorChain nextChain) {
        this.nextChain = nextChain;
    }

    @Override
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
