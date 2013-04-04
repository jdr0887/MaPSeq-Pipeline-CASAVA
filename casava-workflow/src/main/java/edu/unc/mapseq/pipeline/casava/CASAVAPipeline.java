package edu.unc.mapseq.pipeline.casava;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.concurrent.Executors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

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
import edu.unc.mapseq.pipeline.AbstractPipeline;
import edu.unc.mapseq.pipeline.PipelineException;
import edu.unc.mapseq.pipeline.PipelineJobFactory;

public class CASAVAPipeline extends AbstractPipeline<CASAVAPipelineBeanService> {

    private final Logger logger = LoggerFactory.getLogger(CASAVAPipeline.class);

    private CASAVAPipelineBeanService pipelineBeanService;

    public CASAVAPipeline() {
        super();
    }

    @Override
    public String getName() {
        return CASAVAPipeline.class.getSimpleName().replace("Pipeline", "");
    }

    @Override
    public String getVersion() {
        ResourceBundle ncgenesBundle = ResourceBundle.getBundle("edu/unc/mapseq/pipeline/casava/pipeline");
        String version = ncgenesBundle.getString("version");
        return StringUtils.isNotEmpty(version) ? version : "0.0.1-SNAPSHOT";
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws PipelineException {
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
            htsfSampleList = this.pipelineBeanService.getMaPSeqDAOBean().getHTSFSampleDAO()
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

            for (Integer laneIndex : laneMap.keySet()) {

                try {

                    File unalignedDir = new File(sequencerRunDir, String.format("%s.%d", "Unaligned", laneIndex));

                    CondorJob configureBCLToFastQJob = PipelineJobFactory.createJob(++count,
                            ConfigureBCLToFastqCLI.class, getWorkflowPlan());
                    configureBCLToFastQJob.addArgument(ConfigureBCLToFastqCLI.INPUTDIR, baseCallsDir.getAbsolutePath());
                    configureBCLToFastQJob.addArgument(ConfigureBCLToFastqCLI.MISMATCHES);
                    configureBCLToFastQJob.addArgument(ConfigureBCLToFastqCLI.IGNOREMISSINGBCL);
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
                        throw new PipelineException("Invalid SampleSheet: ");
                    }

                    configureBCLToFastQJob.addArgument(ConfigureBCLToFastqCLI.SAMPLESHEET,
                            sampleSheetFile.getAbsolutePath());
                    configureBCLToFastQJob.addArgument(ConfigureBCLToFastqCLI.FORCE);
                    graph.addVertex(configureBCLToFastQJob);

                    if (unalignedDir.exists()) {
                        CondorJob removeUnalignedDirectoryJob = PipelineJobFactory.createJob(++count, RemoveCLI.class,
                                getWorkflowPlan());
                        removeUnalignedDirectoryJob.addArgument(RemoveCLI.FILE, unalignedDir);
                        graph.addVertex(removeUnalignedDirectoryJob);
                        graph.addEdge(removeUnalignedDirectoryJob, configureBCLToFastQJob);
                    }

                    CondorJob makeJob = PipelineJobFactory.createJob(++count, MakeCLI.class, getWorkflowPlan(), null);
                    makeJob.setNumberOfProcessors(2);
                    makeJob.addArgument(MakeCLI.THREADS, "2");
                    makeJob.addArgument(MakeCLI.WORKDIR, unalignedDir.getAbsolutePath());
                    graph.addVertex(makeJob);
                    graph.addEdge(configureBCLToFastQJob, makeJob);

                    logger.debug("readCount = {}", readCount);

                    for (HTSFSample htsfSample : laneMap.get(laneIndex)) {

                        SequencerRun sequencerRun = htsfSample.getSequencerRun();
                        File outputDirectory = createOutputDirectory(sequencerRun.getName(), htsfSample.getName(),
                                getName());
                        File tmpDir = new File(outputDirectory, "tmp");
                        tmpDir.mkdirs();

                        logger.info("outputDirectory.getAbsolutePath(): {}", outputDirectory.getAbsolutePath());

                        File projectDirectory = new File(unalignedDir, "Project_" + htsfSample.getStudy().getName());
                        File sampleDirectory = new File(projectDirectory, "Sample_" + htsfSample.getName());

                        CondorJob copyJob = null;
                        File sourceFile = null;
                        File outputFile = null;
                        String outputFileName = null;

                        switch (readCount) {
                            case 1:
                                copyJob = PipelineJobFactory.createJob(++count, CopyCLI.class, getWorkflowPlan(),
                                        htsfSample);
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
                                copyJob = PipelineJobFactory.createJob(++count, CopyCLI.class, getWorkflowPlan(),
                                        htsfSample);
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
                                copyJob = PipelineJobFactory.createJob(++count, CopyCLI.class, getWorkflowPlan(),
                                        htsfSample);
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
                    throw new PipelineException(e);
                }

            }

        }

        return graph;
    }

    @Override
    public void postRun() throws PipelineException {

        List<HTSFSample> htsfSampleList = null;
        try {
            htsfSampleList = pipelineBeanService.getMaPSeqDAOBean().getHTSFSampleDAO()
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

        SaveDemultiplexedStatsAttributesRunnable saveDemultiplexedStatsAttributesRunnable = new SaveDemultiplexedStatsAttributesRunnable();
        saveDemultiplexedStatsAttributesRunnable.setMapseqDAOBean(pipelineBeanService.getMaPSeqDAOBean());
        saveDemultiplexedStatsAttributesRunnable.setSequencerRunIdList(sequencerRunIdList);
        Executors.newSingleThreadExecutor().execute(saveDemultiplexedStatsAttributesRunnable);

        SaveObservedClusterDensityAttributesRunnable saveObservedClusterDensityAttributesRunnable = new SaveObservedClusterDensityAttributesRunnable();
        saveObservedClusterDensityAttributesRunnable.setMapseqDAOBean(pipelineBeanService.getMaPSeqDAOBean());
        saveObservedClusterDensityAttributesRunnable.setMapseqConfigurationService(pipelineBeanService
                .getMapseqConfigurationService());
        saveObservedClusterDensityAttributesRunnable.setSequencerRunIdList(sequencerRunIdList);
        Executors.newSingleThreadExecutor().execute(saveObservedClusterDensityAttributesRunnable);

    }

    public CASAVAPipelineBeanService getPipelineBeanService() {
        return pipelineBeanService;
    }

    public void setPipelineBeanService(CASAVAPipelineBeanService pipelineBeanService) {
        this.pipelineBeanService = pipelineBeanService;
    }

}
