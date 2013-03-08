package edu.unc.mapseq.main;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.PlatformDAO;
import edu.unc.mapseq.dao.model.Platform;
import edu.unc.mapseq.dao.ws.WebServiceDAOManager;

public class ListPlatformsTest {

    @Test
    public void testRun() {

        String platformName = "";
        String platformInstrument = "";

        // String platformName = "ILLUMINA";
        // String platformInstrument = "";

        // String platformName = "ILLUMINA";
        // String platformInstrument = "Illumina Genome Analyzer II";

        WebServiceDAOManager daoMgr = WebServiceDAOManager.getInstance();
        PlatformDAO platformDAO = daoMgr.getWSDAOBean().getPlatformDAO();

        List<Platform> platformList = new ArrayList<Platform>();

        if (StringUtils.isNotEmpty(platformName) && StringUtils.isNotEmpty(platformInstrument)) {
            try {
                platformList.add(platformDAO.findByInstrumentAndModel(platformName, platformInstrument));
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
            }
        } else if (StringUtils.isNotEmpty(platformName) && StringUtils.isEmpty(platformInstrument)) {
            try {
                platformList.addAll(platformDAO.findByInstrument(platformName));
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
            }
        } else if (StringUtils.isEmpty(platformName) && StringUtils.isEmpty(platformInstrument)) {
            try {
                platformList.addAll(platformDAO.findAll());
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
            }
        }

        if (platformList.size() > 0) {
            StringBuilder sb = new StringBuilder();
            Formatter formatter = new Formatter(sb, Locale.US);
            formatter.format("%1$-5s %2$-20s %3$s%n", "ID", "Instrument", "Model");
            for (Platform platform : platformList) {
                formatter.format("%1$-5s %2$-20s %3$s%n", platform.getId(), platform.getInstrument(),
                        platform.getInstrumentModel());
            }
            System.out.println(formatter.toString());
            formatter.close();
        }

    }

}
