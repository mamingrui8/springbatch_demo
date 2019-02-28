package com.springbatch.demo.step;

import java.util.List;
import org.springframework.batch.item.ItemWriter;

/**
 *  对Reader提供的Items进行加工
 */
public class Writer implements ItemWriter<String> {

    @Override
    public void write(List<? extends String> messages) throws Exception {
        for (String msg : messages) {
            System.out.println("Writing the data " + msg);
        }
    }

}
