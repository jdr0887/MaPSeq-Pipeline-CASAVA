package edu.unc.mapseq.workflow.casava;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.casava.FixMismappedFastqFileDataRunnable;
import edu.unc.mapseq.commons.casava.SaveDemultiplexedStatsAttributesRunnable;
import edu.unc.mapseq.commons.casava.SaveObservedClusterDensityAttributesRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.EntityAttribute;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.module.casava.ConfigureBCLToFastqCLI;
import edu.unc.mapseq.module.core.CopyCLI;
import edu.unc.mapseq.module.core.MakeCLI;
import edu.unc.mapseq.module.core.RemoveCLI;
import edu.unc.mapseq.workflow.AbstractWorkflow;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.WorkflowJobFactory;

public class CASAVAWorkflow extends AbstractWorkflow {

    private final Logger logger = LoggerFactory.getLogger(CASAVAWorkflow.class);

    public CASAVAWorkflow() {
        super();
    }

    @Override
    public String getName() {
        return CASAVAWorkflow.class.getSimpleName().replace("Workflow", "");
    }

    @Override
    public String getVersion() {
        ResourceBundle ncgenesBundle = ResourceBundle.getBundle("edu/unc/mapseq/workflow/casava/workflow");
        String version = ncgenesBundle.getString("version");
        return StringUtils.isNotEmpty(version) ? version : "0.0.1-SNAPSHOT";
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws WorkflowException {
        logger.debug("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        int count = 0;

        File baseDir = new File(getWorkflowPlan().getSequencerRun().getBaseDirectory());
        File sequencerRunDir = new File(baseDir, getWorkflowPlan().getSequencerRun().getName());
        File dataDir = new File(sequencerRunDir, "Data");
        File intensitiesDir = new File(dataDir, "Intensities");
        File baseCallsDir = new File(intensitiesDir, "BaseCalls");

        List<HTSFSample> htsfSampleList = null;
        try {
            htsfSampleList = getWorkflowBeanService().getMaPSeqDAOBean().getHTSFSampleDAO()
                    .findBySequencerRunId(getWorkflowPlan().getSequencerRun().getId());
        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

        Map<Integer, List<HTSFSample>> laneMap = new HashMap<Integer, List<HTSFSample>>();

        if (htsfSampleList != null && htsfSampleList.size() > 0) {

            logger.info("htsfSampleList.size() = {}", htsfSampleList.size());

            for (HTSFSample htsfSample : htsfSampleList) {
                if (laneMap.containsKey(htsfSample.getLaneIndex()) || "Undetermined".equals(htsfSample.getBarcode())) {
                    continue;
                }
                laneMap.put(htsfSample.getLaneIndex(), new ArrayList<HTSFSample>());
            }

            for (HTSFSample htsfSample : htsfSampleList) {
                if ("Undetermined".equals(htsfSample.getBarcode())) {
                    continue;
                }
                laneMap.get(htsfSample.getLaneIndex()).add(htsfSample);
            }
        }

        if (laneMap.size() > 0) {

            String siteName = getWorkflowBeanService().getAttributes().get("siteName");

            for (Integer laneIndex : laneMap.keySet()) {

                try {

                    File unalignedDir = new File(sequencerRunDir, String.format("%s.%d", "Unaligned", laneIndex));

                    CondorJob configureBCLToFastQJob = WorkflowJobFactory.createJob(++count,
                            ConfigureBCLToFastqCLI.class, getWorkflowPlan());
                    configureBCLToFastQJob.setSiteName(siteName);
                    configureBCLToFastQJob.addArgument(ConfigureBCLToFastqCLI.INPUTDIR, baseCallsDir.getAbsolutePath());
                    configureBCLToFastQJob.addArgument(ConfigureBCLToFastqCLI.MISMATCHES);
                    configureBCLToFastQJob.addArgument(ConfigureBCLToFastqCLI.IGNOREMISSINGBCL);
                    configureBCLToFastQJob.addArgument(ConfigureBCLToFastqCLI.IGNOREMISSINGSTATS);
                    configureBCLToFastQJob.addArgument(ConfigureBCLToFastqCLI.FASTQCLUSTERCOUNT, "0");
                    configureBCLToFastQJob.addArgument(ConfigureBCLToFastqCLI.TILES, laneIndex.toString());
                    configureBCLToFastQJob
                            .addArgument(ConfigureBCLToFastqCLI.OUTPUTDIR, unalignedDir.getAbsolutePath());

                    Set<EntityAttribute> srEntityAttributeSet = getWorkflowPlan().getSequencerRun().getAttributes();

                    File sampleSheetFile = null;
                    Integer readCount = null;

                    if (srEntityAttributeSet != null) {
                        Iterator<EntityAttribute> srEntityAttributeIter = srEntityAttributeSet.iterator();
                        while (srEntityAttributeIter.hasNext()) {
                            EntityAttribute ea = srEntityAttributeIter.next();
                            if ("sampleSheet".equals(ea.getName())) {
                                sampleSheetFile = new File(ea.getValue());
                            }
                            if ("readCount".equals(ea.getName())) {
                                readCount = Integer.valueOf(ea.getValue());
                            }
                        }
                    }

                    if (sampleSheetFile == null) {
                        sampleSheetFile = new File(baseCallsDir, "SampleSheet.csv");
                    }

                    if (!sampleSheetFile.exists()) {
                        logger.error("Specified sample sheet doesn't exist: {}", sampleSheetFile.getAbsolutePath());
                        throw new WorkflowException("Invalid SampleSheet: ");
                    }

                    configureBCLToFastQJob.addArgument(ConfigureBCLToFastqCLI.SAMPLESHEET,
                            sampleSheetFile.getAbsolutePath());
                    configureBCLToFastQJob.addArgument(ConfigureBCLToFastqCLI.FORCE);
                    graph.addVertex(configureBCLToFastQJob);

                    if (unalignedDir.exists()) {
                        CondorJob removeUnalignedDirectoryJob = WorkflowJobFactory.createJob(++count, RemoveCLI.class,
                                getWorkflowPlan());
                        removeUnalignedDirectoryJob.setSiteName(getWorkflowBeanService().getAttributes()
                                .get("siteName"));
                        removeUnalignedDirectoryJob.addArgument(RemoveCLI.FILE, unalignedDir);
                        graph.addVertex(removeUnalignedDirectoryJob);
                        graph.addEdge(removeUnalignedDirectoryJob, configureBCLToFastQJob);
                    }

                    CondorJob makeJob = WorkflowJobFactory.createJob(++count, MakeCLI.class, getWorkflowPlan(), null);
                    makeJob.setSiteName(siteName);
                    makeJob.setNumberOfProcessors(2);
                    makeJob.addArgument(MakeCLI.THREADS, "2");
                    makeJob.addArgument(MakeCLI.WORKDIR, unalignedDir.getAbsolutePath());
                    graph.addVertex(makeJob);
                    graph.addEdge(configureBCLToFastQJob, makeJob);

                    logger.debug("readCount = {}", readCount);

                    for (HTSFSample htsfSample : laneMap.get(laneIndex)) {

                        SequencerRun sequencerRun = htsfSample.getSequencerRun();
                        File outputDirectory = createOutputDirectory(sequencerRun.getName(), htsfSample, getName(),
                                getVersion());

                        logger.info("outputDirectory.getAbsolutePath(): {}", outputDirectory.getAbsolutePath());

                        File projectDirectory = new File(unalignedDir, "Project_" + htsfSample.getStudy().getName());
                        File sampleDirectory = new File(projectDirectory, "Sample_" + htsfSample.getName());

                        CondorJob copyJob = null;
                        File sourceFile = null;
                        File outputFile = null;
                        String outputFileName = null;

                        switch (readCount) {
                            case 1:
                                copyJob = WorkflowJobFactory.createJob(++count, CopyCLI.class, getWorkflowPlan(),
                                        htsfSample);
                                copyJob.setSiteName(siteName);
                                sourceFile = new File(sampleDirectory, String.format("%s_%s_L%03d_R%d_001.fastq.gz",
                                        htsfSample.getName(), htsfSample.getBarcode(), laneIndex, 1));
                                copyJob.addArgument(CopyCLI.SOURCE, sourceFile.getAbsolutePath());
                                outputFileName = String.format("%s_%s_L%03d_R%d.fastq.gz", getWorkflowPlan()
                                        .getSequencerRun().getName(), htsfSample.getBarcode(), laneIndex, 1);
                                outputFile = new File(outputDirectory, outputFileName);
                                copyJob.addArgument(CopyCLI.DESTINATION, outputFile.getAbsolutePath());
                                copyJob.addArgument(CopyCLI.MIMETYPE, MimeType.FASTQ.toString());
                                graph.addVertex(copyJob);
                                graph.addEdge(makeJob, copyJob);

                                break;
                            case 2:
                            default:

                                // read 1
                                copyJob = WorkflowJobFactory.createJob(++count, CopyCLI.class, getWorkflowPlan(),
                                        htsfSample);
                                copyJob.setSiteName(siteName);
                                sourceFile = new File(sampleDirectory, String.format("%s_%s_L%03d_R%d_001.fastq.gz",
                                        htsfSample.getName(), htsfSample.getBarcode(), laneIndex, 1));
                                copyJob.addArgument(CopyCLI.SOURCE, sourceFile.getAbsolutePath());
                                outputFileName = String.format("%s_%s_L%03d_R%d.fastq.gz", getWorkflowPlan()
                                        .getSequencerRun().getName(), htsfSample.getBarcode(), laneIndex, 1);
                                outputFile = new File(outputDirectory, outputFileName);
                                copyJob.addArgument(CopyCLI.DESTINATION, outputFile.getAbsolutePath());
                                copyJob.addArgument(CopyCLI.MIMETYPE, MimeType.FASTQ.toString());
                                graph.addVertex(copyJob);
                                graph.addEdge(makeJob, copyJob);

                                // read 2
                                copyJob = WorkflowJobFactory.createJob(++count, CopyCLI.class, getWorkflowPlan(),
                                        htsfSample);
                                copyJob.setSiteName(siteName);
                                sourceFile = new File(sampleDirectory, String.format("%s_%s_L%03d_R%d_001.fastq.gz",
                                        htsfSample.getName(), htsfSample.getBarcode(), laneIndex, 2));
                                copyJob.addArgument(CopyCLI.SOURCE, sourceFile.getAbsolutePath());
                                outputFileName = String.format("%s_%s_L%03d_R%d.fastq.gz", getWorkflowPlan()
                                        .getSequencerRun().getName(), htsfSample.getBarcode(), laneIndex, 2);
                                outputFile = new File(outputDirectory, outputFileName);
                                copyJob.addArgument(CopyCLI.DESTINATION, outputFile.getAbsolutePath());
                                copyJob.addArgument(CopyCLI.MIMETYPE, MimeType.FASTQ.toString());
                                graph.addVertex(copyJob);
                                graph.addEdge(makeJob, copyJob);

                                break;
                        }

                    }
                } catch (Exception e) {
                    throw new WorkflowException(e);
                }

            }

        }

        return graph;
    }

    @Override
    public void postRun() throws WorkflowException {

        List<HTSFSample> htsfSampleList = null;
        try {
            htsfSampleList = getWorkflowBeanService().getMaPSeqDAOBean().getHTSFSampleDAO()
                    .findBySequencerRunId(getWorkflowPlan().getSequencerRun().getId());
        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

        if (htsfSampleList == null) {
            logger.warn("htsfSampleList was null");
            return;
        }

        List<Long> sequencerRunIdList = new ArrayList<Long>();
        sequencerRunIdList.add(getWorkflowPlan().getSequencerRun().getId());

        FixMismappedFastqFileDataRunnable fixMismappedFastqFileDataRunnable = new FixMismappedFastqFileDataRunnable();
        fixMismappedFastqFileDataRunnable.setMapseqDAOBean(getWorkflowBeanService().getMaPSeqDAOBean());
        fixMismappedFastqFileDataRunnable.setSequencerRunIdList(sequencerRunIdList);
        Executors.newSingleThreadExecutor().execute(fixMismappedFastqFileDataRunnable);

        SaveDemultiplexedStatsAttributesRunnable saveDemultiplexedStatsAttributesRunnable = new SaveDemultiplexedStatsAttributesRunnable();
        saveDemultiplexedStatsAttributesRunnable.setMapseqDAOBean(getWorkflowBeanService().getMaPSeqDAOBean());
        saveDemultiplexedStatsAttributesRunnable.setSequencerRunIdList(sequencerRunIdList);
        Executors.newSingleThreadExecutor().execute(saveDemultiplexedStatsAttributesRunnable);

        SaveObservedClusterDensityAttributesRunnable saveObservedClusterDensityAttributesRunnable = new SaveObservedClusterDensityAttributesRunnable();
        saveObservedClusterDensityAttributesRunnable.setMapseqDAOBean(getWorkflowBeanService().getMaPSeqDAOBean());
        saveObservedClusterDensityAttributesRunnable.setMapseqConfigurationService(getWorkflowBeanService()
                .getMaPSeqConfigurationService());
        saveObservedClusterDensityAttributesRunnable.setSequencerRunIdList(sequencerRunIdList);
        Executors.newSingleThreadExecutor().execute(saveObservedClusterDensityAttributesRunnable);

    }

}
