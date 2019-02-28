package com.springbatch.demo.step2;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

public class SampleStep2Reader implements ItemReader {
    @Override
    public Object read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return null;
    }
}
