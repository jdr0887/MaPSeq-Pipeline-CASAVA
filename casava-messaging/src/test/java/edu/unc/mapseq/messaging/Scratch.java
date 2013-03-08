package edu.unc.mapseq.messaging;

import org.junit.Test;

public class Scratch {

    @Test
    public void testModulus() {

        for (int i = 0; i < 186; ++i) {
            if ((i % 4) == 0) {
                System.out.println(i);
            }
        }

    }
}
