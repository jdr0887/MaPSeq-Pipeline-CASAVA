package edu.unc.mapseq.pipeline.casava;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class Scratch {

    @Test
    public void testPadding() {

        Integer laneIndex = 2;
        String format = "%03d";
        assertTrue("002".equals(String.format(format, laneIndex)));

        Properties props = new Properties();
        try {
            props.load(this.getClass().getResourceAsStream("pipeline.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        assertTrue("0.0.1-SNAPSHOT".equals(props.getProperty("version", null)));

    }

    @Test
    public void testPatternMatchingForFileData() {
        String filename = "120110_UNC13-SN749_0141_AD0J7WACXX_GGCTAC_L002_R2.fastq";
        Pattern patternR1 = Pattern.compile("^120110_UNC13-SN749_0141_AD0J7WACXX.*_L002_R2\\.fastq$");
        Matcher matcherR1 = patternR1.matcher(filename);
        assertTrue(matcherR1.matches());
    }

}
