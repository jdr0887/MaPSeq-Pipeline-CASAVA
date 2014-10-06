package edu.unc.mapseq.commons.casava;

import java.io.File;
import java.util.ArrayList;
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

            if (!fList.isEmpty()) {

                for (Flowcell sr : fList) {

                    List<Sample> sampleList = sampleDAO.findByFlowcellId(sr.getId());

                    if (sampleList == null) {
                        logger.warn("sampleList was null");
                        continue;
                    }

                    for (Sample sample : sampleList) {

                        logger.debug("{}", sample.toString());

                        File read1File = new File(sample.getOutputDirectory(),
                                String.format("%s_%s_L%03d_R%d.fastq.gz", sr.getName(), sample.getBarcode(),
                                        sample.getLaneIndex(), 1));

                        List<FileData> potentialRead1FastqFiles = fileDataDAO.findByExample(new FileData(read1File
                                .getName(), read1File.getParentFile().getAbsolutePath(), MimeType.FASTQ));

                        if (potentialRead1FastqFiles == null
                                || (potentialRead1FastqFiles != null && potentialRead1FastqFiles.isEmpty())) {
                            logger.warn("read1Fastq not found");
                            logger.warn(sample.toString());
                            break;
                        }

                        File read2File = new File(sample.getOutputDirectory(),
                                String.format("%s_%s_L%03d_R%d.fastq.gz", sr.getName(), sample.getBarcode(),
                                        sample.getLaneIndex(), 2));

                        List<FileData> potentialRead2FastqFiles = fileDataDAO.findByExample(new FileData(read2File
                                .getName(), read2File.getParentFile().getAbsolutePath(), MimeType.FASTQ));

                        if (potentialRead2FastqFiles == null
                                || (potentialRead2FastqFiles != null && potentialRead2FastqFiles.isEmpty())) {
                            logger.warn("read1Fastq not found");
                            logger.warn(sample.toString());
                            break;
                        }

                        if ((potentialRead1FastqFiles != null && !potentialRead1FastqFiles.isEmpty())
                                && (potentialRead2FastqFiles != null && !potentialRead2FastqFiles.isEmpty())) {

                            Set<FileData> fileDataSet = sample.getFileDatas();

                            FileData read1FastqFileData = potentialRead1FastqFiles.get(0);
                            FileData read2FastqFileData = potentialRead2FastqFiles.get(0);

                            if (fileDataSet.contains(read1FastqFileData) && fileDataSet.contains(read2FastqFileData)) {
                                continue;
                            }

                            if (!fileDataSet.contains(read1FastqFileData) && fileDataSet.contains(read2FastqFileData)) {
                                fileDataSet.add(read1FastqFileData);
                            }

                            if (fileDataSet.contains(read1FastqFileData) && !fileDataSet.contains(read2FastqFileData)) {
                                fileDataSet.add(read2FastqFileData);
                            }

                            sample.setFileDatas(fileDataSet);
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
