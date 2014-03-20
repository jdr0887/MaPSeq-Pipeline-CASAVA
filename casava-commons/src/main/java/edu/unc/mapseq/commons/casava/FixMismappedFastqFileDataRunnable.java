package edu.unc.mapseq.commons.casava;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SequencerRunDAO;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.SequencerRun;

public class FixMismappedFastqFileDataRunnable implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(FixMismappedFastqFileDataRunnable.class);

    private MaPSeqDAOBean mapseqDAOBean;

    private List<Long> sequencerRunIdList;

    public FixMismappedFastqFileDataRunnable() {
        super();
    }

    @Override
    public void run() {
        logger.debug("ENTERING run()");

        List<SequencerRun> srList = new ArrayList<SequencerRun>();

        try {

            SequencerRunDAO sequencerRunDAO = mapseqDAOBean.getSequencerRunDAO();
            for (Long sequencerRunId : sequencerRunIdList) {
                SequencerRun sequencerRun = sequencerRunDAO.findById(sequencerRunId);
                if (sequencerRun != null) {
                    srList.add(sequencerRun);
                }
            }

            String outputDir = System.getenv("MAPSEQ_OUTPUT_DIRECTORY");

            if (srList.size() > 0) {

                for (SequencerRun sr : srList) {

                    List<HTSFSample> htsfSampleList = mapseqDAOBean.getHTSFSampleDAO().findBySequencerRunId(sr.getId());

                    if (htsfSampleList == null) {
                        logger.warn("htsfSampleList was null");
                        continue;
                    }

                    for (HTSFSample sample : htsfSampleList) {

                        logger.debug("{}", sample.toString());

                        Set<FileData> fileDataSet = sample.getFileDatas();
                        Set<FileData> fastqFileDataSet = new HashSet<FileData>();

                        for (FileData fileData : fileDataSet) {
                            logger.debug("fileData: {}", fileData);
                            String path = String.format("%s/%s/CASAVA", outputDir, sr.getName());
                            logger.debug("path: {}", path);
                            if (MimeType.FASTQ.equals(fileData.getMimeType())
                                    && fileData.getName().endsWith("fastq.gz") && fileData.getPath().startsWith(path)) {
                                fastqFileDataSet.add(fileData);
                            }
                        }

                        logger.debug("fastqFileDataSet.size(): {}", fastqFileDataSet.size());

                        if (fastqFileDataSet.size() == 2) {
                            continue;
                        }

                        if (fastqFileDataSet.size() == 1) {
                            // mismapped
                            FileData oneOfFastqPair = fastqFileDataSet.iterator().next();
                            logger.debug("found mismapped: {}", oneOfFastqPair.toString());
                            FileData tmpFastqFileData = new FileData();
                            tmpFastqFileData.setMimeType(MimeType.FASTQ);
                            tmpFastqFileData.setPath(oneOfFastqPair.getPath());
                            List<FileData> results = mapseqDAOBean.getFileDataDAO().findByExample(tmpFastqFileData);
                            FileData tmpFileData = null;
                            if (results.size() == 2) {
                                // get the one not already mapped
                                FileData first = results.get(0);
                                FileData second = results.get(1);
                                if (oneOfFastqPair.equals(first)) {
                                    tmpFileData = second;
                                }
                                if (oneOfFastqPair.equals(second)) {
                                    tmpFileData = first;
                                }
                            }
                            // we now have the mismapped fastq file...attach to sample
                            sample.getFileDatas().add(tmpFileData);
                            mapseqDAOBean.getHTSFSampleDAO().save(sample);
                        }

                    }

                }

            }

        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public MaPSeqDAOBean getMapseqDAOBean() {
        return mapseqDAOBean;
    }

    public void setMapseqDAOBean(MaPSeqDAOBean mapseqDAOBean) {
        this.mapseqDAOBean = mapseqDAOBean;
    }

    public List<Long> getSequencerRunIdList() {
        return sequencerRunIdList;
    }

    public void setSequencerRunIdList(List<Long> sequencerRunIdList) {
        this.sequencerRunIdList = sequencerRunIdList;
    }

}
