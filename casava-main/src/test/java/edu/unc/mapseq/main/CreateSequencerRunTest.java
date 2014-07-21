package edu.unc.mapseq.main;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.PlatformDAO;
import edu.unc.mapseq.dao.model.Account;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.Platform;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.dao.model.Study;
import edu.unc.mapseq.dao.ws.WSDAOManager;

public class CreateSequencerRunTest {

    @Test
    public void testFileNameRegex() {
        String good = "120904_UNC14-SN744_0267_BD15MRACXX";
        Pattern pattern = Pattern.compile("^\\d+_.+_\\d+_.+_\\w+$");
        Matcher matcher = pattern.matcher(good);
        assertTrue(matcher.matches());

        String bad = "TestSequencerRun";
        matcher = pattern.matcher(bad);
        assertFalse(matcher.matches());
    }

    @Test
    public void testRun() {

        WSDAOManager daoMgr = WSDAOManager.getInstance();

        try {
            String sampleSheetContent = IOUtils.toString(this.getClass().getClassLoader()
                    .getResourceAsStream("edu/unc/mapseq/main/SampleSheet.csv"));
            LineNumberReader lnr = new LineNumberReader(new StringReader(sampleSheetContent));
            lnr.readLine();
            String line;

            MaPSeqDAOBean maPSeqDAOBean = daoMgr.getMaPSeqDAOBean();

            List<Account> accountList = null;
            try {
                accountList = maPSeqDAOBean.getAccountDAO().findByName(System.getProperty("user.name"));
                if (accountList == null || (accountList != null && accountList.isEmpty())) {
                    System.err.printf("Account doesn't exist: %s%n", System.getProperty("user.name"));
                    System.err.println("Must register account first");
                    return;
                }
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
            }

            Account account = accountList.get(0);

            Platform platform = null;
            try {
                PlatformDAO platformDAO = daoMgr.getMaPSeqDAOBean().getPlatformDAO();
                platform = platformDAO.findById(53L);
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
            }

            SequencerRun sequencerRun = new SequencerRun();
            sequencerRun.setCreator(account);
            sequencerRun.setBaseDirectory("asdfasdf");
            sequencerRun.setPlatform(platform);

            try {
                Long sequencerRunId = daoMgr.getMaPSeqDAOBean().getSequencerRunDAO().save(sequencerRun);
                sequencerRun.setId(sequencerRunId);
            } catch (MaPSeqDAOException e1) {
                e1.printStackTrace();
            }

            while ((line = lnr.readLine()) != null) {

                String[] st = line.split(",");
                String flowcell = st[0];
                String laneIndex = st[1];
                String sampleId = st[2];
                String sampleRef = st[3];
                String index = st[4];
                String description = st[5];
                String control = st[6];
                String recipe = st[7];
                String operator = st[8];
                String sampleProject = st[9];

                if (StringUtils.isEmpty(sampleProject)) {
                    System.err.printf("SampleProject is empty");
                    return;
                }
                Study study = daoMgr.getMaPSeqDAOBean().getStudyDAO().findByName(sampleProject).get(0);
                if (study == null) {
                    study = new Study();
                    study.setApproved(Boolean.TRUE);
                    study.setCreator(account);
                    study.setGrant("test");
                    study.setName(sampleProject);
                    daoMgr.getMaPSeqDAOBean().getStudyDAO().save(study);
                }

                HTSFSample htsfSample = new HTSFSample();
                htsfSample.setBarcode(index);
                htsfSample.setCreator(account);
                htsfSample.setLaneIndex(Integer.valueOf(laneIndex));
                htsfSample.setName(sampleId);
                // htsfSample.setSequencerRun(sequencerRun);
                htsfSample.setStudy(study);

                daoMgr.getMaPSeqDAOBean().getHTSFSampleDAO().save(htsfSample);
            }

        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
