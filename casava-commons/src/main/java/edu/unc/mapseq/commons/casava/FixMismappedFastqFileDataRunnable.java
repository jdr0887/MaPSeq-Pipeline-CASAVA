package edu.unc.mapseq.commons.casava;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
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

                        Set<FileData> sampleFiles = sample.getFileDatas();

                        File casavaDirectory = new File(sample.getOutputDirectory(), "CASAVA");

                        File expectedRead1FastqFile = new File(casavaDirectory.getAbsolutePath(),
                                String.format("%s_%s_L%03d_R%d.fastq.gz", sr.getName(), sample.getBarcode(),
                                        sample.getLaneIndex(), 1));

                        File expectedRead2FastqFile = new File(casavaDirectory.getAbsolutePath(),
                                String.format("%s_%s_L%03d_R%d.fastq.gz", sr.getName(), sample.getBarcode(),
                                        sample.getLaneIndex(), 2));

                        int foundR1FastqCount = 0;
                        int foundR2FastqCount = 0;
                        for (FileData fd : sampleFiles) {
                            if (fd.getPath().equals(casavaDirectory.getAbsolutePath())) {
                                if (fd.getName().equals(expectedRead1FastqFile.getName())) {
                                    ++foundR1FastqCount;
                                }
                                if (fd.getName().equals(expectedRead2FastqFile.getName())) {
                                    ++foundR2FastqCount;
                                }
                            }
                        }

                        if (foundR1FastqCount == 1 && foundR2FastqCount == 1) {
                            continue;
                        }

                        // read1fastq not found...need to find/create & add to sample
                        if (foundR1FastqCount == 0) {

                            FileData fileData = new FileData(expectedRead1FastqFile.getName(),
                                    casavaDirectory.getAbsolutePath(), MimeType.FASTQ);
                            List<FileData> potentialFastqFiles = fileDataDAO.findByExample(fileData);

                            // found, but not mapped
                            if ((potentialFastqFiles != null && !potentialFastqFiles.isEmpty())) {
                                FileData fastqFileData = potentialFastqFiles.get(0);
                                if (!sampleFiles.contains(fastqFileData)) {
                                    sampleFiles.add(fastqFileData);
                                }
                            }

                            // not found
                            if (potentialFastqFiles == null
                                    || (potentialFastqFiles != null && potentialFastqFiles.isEmpty())) {
                                fileData.setId(fileDataDAO.save(fileData));
                                sampleFiles.add(fileData);
                            }

                        }

                        // read2fastq not found...need to find/create & add to sample
                        if (foundR2FastqCount == 0) {

                            FileData fileData = new FileData(expectedRead2FastqFile.getName(),
                                    casavaDirectory.getAbsolutePath(), MimeType.FASTQ);
                            List<FileData> potentialFastqFiles = fileDataDAO.findByExample(fileData);

                            // found, but not mapped
                            if ((potentialFastqFiles != null && !potentialFastqFiles.isEmpty())) {
                                FileData fastqFileData = potentialFastqFiles.get(0);
                                if (!sampleFiles.contains(fastqFileData)) {
                                    sampleFiles.add(fastqFileData);
                                }
                            }

                            // not found
                            if (potentialFastqFiles == null
                                    || (potentialFastqFiles != null && potentialFastqFiles.isEmpty())) {
                                fileData.setId(fileDataDAO.save(fileData));
                                sampleFiles.add(fileData);
                            }

                        }

                        // found duplicate
                        if (foundR1FastqCount > 1) {

                            List<FileData> fastqFiles = new ArrayList<FileData>();
                            for (FileData fd : sampleFiles) {
                                if (!fd.getName().equals(expectedRead1FastqFile.getName())
                                        && !fd.getPath().equals(casavaDirectory.getAbsolutePath())) {
                                    continue;
                                }
                                fastqFiles.add(fd);
                            }

                            List<FileData> sorted = new ArrayList<FileData>(fastqFiles);
                            Collections.sort(sorted, new Comparator<FileData>() {
                                @Override
                                public int compare(FileData one, FileData two) {
                                    return one.getId().compareTo(two.getId());
                                }
                            });

                            FileData toKeep = sorted.get(0);
                            Iterator<FileData> iter = sampleFiles.iterator();
                            while (iter.hasNext()) {
                                FileData next = iter.next();
                                if (next.getName().equals(toKeep.getName()) && next.getPath().equals(toKeep.getPath())
                                        && next.getId() > toKeep.getId()) {
                                    iter.remove();
                                }
                            }
                        }

                        // found duplicate
                        if (foundR2FastqCount > 1) {
                            List<FileData> fastqFiles = new ArrayList<FileData>();
                            for (FileData fd : sampleFiles) {
                                if (!fd.getName().equals(expectedRead2FastqFile.getName())
                                        && !fd.getPath().equals(casavaDirectory.getAbsolutePath())) {
                                    continue;
                                }
                                fastqFiles.add(fd);
                            }

                            List<FileData> sorted = new ArrayList<FileData>(fastqFiles);
                            Collections.sort(sorted, new Comparator<FileData>() {
                                @Override
                                public int compare(FileData one, FileData two) {
                                    return one.getId().compareTo(two.getId());
                                }
                            });

                            FileData toKeep = sorted.get(0);
                            Iterator<FileData> iter = sampleFiles.iterator();
                            while (iter.hasNext()) {
                                FileData next = iter.next();
                                if (next.getName().equals(toKeep.getName()) && next.getPath().equals(toKeep.getPath())
                                        && next.getId() > toKeep.getId()) {
                                    iter.remove();
                                }
                            }
                        }

                        sample.setFileDatas(sampleFiles);
                        sampleDAO.save(sample);

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
