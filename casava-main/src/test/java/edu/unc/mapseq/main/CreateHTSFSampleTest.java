package edu.unc.mapseq.main;

import static org.junit.Assert.assertTrue;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class CreateHTSFSampleTest {

    @Test
    public void testFileNameRegex() {
        String good = "120904_UNC14-SN744_0267_BD15MRACXX_ACTTGA_L001_R1.fastq.gz";
        Pattern patternR1 = Pattern.compile("^\\d+_.+_\\d+_.+_L\\d+_R\\d\\.fastq\\.gz$");
        Matcher matcherR1 = patternR1.matcher(good);
        assertTrue(matcherR1.matches());
    }

}
