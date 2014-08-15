package edu.unc.mapseq.commons.casava;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.FileDataDAO;
import edu.unc.mapseq.dao.FlowcellDAO;
import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;

public class FixMismappedFastqFileDataRunnable implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(FixMismappedFastqFileDataRunnable.class);

    private MaPSeqDAOBean mapseqDAOBean;

    private List<Long> flowcellIdList;

    public FixMismappedFastqFileDataRunnable() {
        super();
    }

    @Override
    public void run() {
        logger.debug("ENTERING run()");

        FlowcellDAO flowcellDAO = mapseqDAOBean.getFlowcellDAO();
        SampleDAO sampleDAO = mapseqDAOBean.getSampleDAO();
        FileDataDAO fileDataDAO = mapseqDAOBean.getFileDataDAO();

        List<Flowcell> fList = new ArrayList<Flowcell>();

        try {

            for (Long flowcellId : getFlowcellIdList()) {
                Flowcell flowcell = flowcellDAO.findById(flowcellId);
                if (flowcell != null) {
                    fList.add(flowcell);
                }
            }

            String outputDir = System.getenv("MAPSEQ_OUTPUT_DIRECTORY");

            if (!fList.isEmpty()) {

                for (Flowcell sr : fList) {

                    List<Sample> sampleList = sampleDAO.findByFlowcellId(sr.getId());

                    if (sampleList == null) {
                        logger.warn("sampleList was null");
                        continue;
                    }

                    for (Sample sample : sampleList) {

                        logger.debug("{}", sample.toString());

                        String read1FileName = String.format("%s_%s_L%03d_R%d.fastq.gz", sr.getName(),
                                sample.getBarcode(), sample.getLaneIndex(), 1);

                        String read2FileName = String.format("%s_%s_L%03d_R%d.fastq.gz", sr.getName(),
                                sample.getBarcode(), sample.getLaneIndex(), 2);

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
                            List<FileData> results = fileDataDAO.findByExample(tmpFastqFileData);
                            FileData tmpFileData = null;
                            if (results.size() == 1) {
                                FileData fd = results.get(0);
                                File read1 = new File(fd.getPath(), read1FileName);
                                File read2 = new File(fd.getPath(), read2FileName);
                                if (fd.getName().equals(read1FileName) && read2.exists()) {
                                    tmpFileData = tmpFastqFileData;
                                    tmpFileData.setName(read2FileName);
                                    Long id = fileDataDAO.save(tmpFileData);
                                    tmpFileData.setId(id);
                                }
                                if (fd.getName().equals(read2FileName) && read1.exists()) {
                                    tmpFileData = tmpFastqFileData;
                                    tmpFileData.setName(read1FileName);
                                    Long id = fileDataDAO.save(tmpFileData);
                                    tmpFileData.setId(id);
                                }
                            }
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
                            sampleDAO.save(sample);
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

    public List<Long> getFlowcellIdList() {
        return flowcellIdList;
    }

    public void setFlowcellIdList(List<Long> flowcellIdList) {
        this.flowcellIdList = flowcellIdList;
    }

}
