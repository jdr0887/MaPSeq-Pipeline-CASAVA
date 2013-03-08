package edu.unc.mapseq.main;

import static org.junit.Assert.assertTrue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class Scratch implements Runnable {

    private final static Options cliOptions = new Options();

    private String adapter;

    public Scratch() {
        super();
    }

    @Test
    public void scratch() {
        String version = "2";
        assertTrue("v2,v2".equals(String.format("v%1$s,v%1$s", version)));
    }

    @Override
    public void run() {
        System.out.println(adapter);
    }

    public String getAdapter() {
        return adapter;
    }

    public void setAdapter(String adapter) {
        this.adapter = adapter;
    }

    public static void main(String[] args) {
        // optional
        cliOptions.addOption(OptionBuilder.withArgName("adapter").hasArgs().withLongOpt("adapter").create());
        Scratch s = new Scratch();
        CommandLineParser commandLineParser = new GnuParser();
        try {
            CommandLine commandLine = commandLineParser.parse(cliOptions, args);
            s.setAdapter(StringUtils.join(commandLine.getOptionValues("adapter"), " "));
            s.run();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
