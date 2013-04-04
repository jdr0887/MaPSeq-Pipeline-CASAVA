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

        File baseDir = new File(getWorkflowPlan().getSequencerRun().getBaseDirectory());
        File sequencerRunDir = new File(baseDir, getWorkflowPlan().getSequencerRun().getName());
        String flowcell = lookupFlowcell(sequencerRunDir);

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

        for (HTSFSample sample : htsfSampleList) {
            try {
                saveDemulitplexStatsAttributes(sequencerRunDir, flowcell, sample);
            } catch (Exception e) {
                logger.error("saveDemulitplexStatsAttributes error", e);
            }
            try {
                saveObservedDensityAttributes(sequencerRunDir, flowcell, sample);
            } catch (Exception e) {
                logger.error("saveObservedDensityAttributes error", e);
            }
        }
    }

    private void saveObservedDensityAttributes(File sequencerRunDir, String flowcell, HTSFSample sample) {
        File dataDir = new File(sequencerRunDir, "Data");
        File reportsDir = new File(dataDir, "reports");
        File numClustersByLaneFile = new File(reportsDir, "NumClusters By Lane.txt");
        if (!numClustersByLaneFile.exists()) {
            logger.warn("numClustersByLaneFile does not exist: {}", numClustersByLaneFile.getAbsolutePath());
            return;
        }

        try {
            long clusterDensityTotal = 0;
            int tileCount = 0;
            BufferedReader br = new BufferedReader(new FileReader(numClustersByLaneFile));
            // skip the first 11 lines
            for (int i = 0; i < 11; ++i) {
                br.readLine();
            }
            String line;
            while ((line = br.readLine()) != null) {
                if (sample.getLaneIndex().equals(Integer.valueOf(StringUtils.split(line)[0]))) {
                    clusterDensityTotal += Long.valueOf(StringUtils.split(line)[2]);
                    ++tileCount;
                }
            }

            br.close();

            String value = (double) (clusterDensityTotal / tileCount) / 1000 + "";

            Set<EntityAttribute> attributeSet = sample.getAttributes();

            if (attributeSet == null) {
                attributeSet = new HashSet<EntityAttribute>();
            }

            Set<String> entityAttributeNameSet = new HashSet<String>();

            for (EntityAttribute attribute : attributeSet) {
                entityAttributeNameSet.add(attribute.getName());
            }

            Set<String> synchSet = Collections.synchronizedSet(entityAttributeNameSet);

            if (StringUtils.isNotEmpty(value)) {
                if (synchSet.contains("observedClusterDensity")) {
                    for (EntityAttribute attribute : attributeSet) {
                        if (attribute.getName().equals("observedClusterDensity")) {
                            attribute.setValue(value);
                            break;
                        }
                    }
                } else {
                    attributeSet.add(new EntityAttribute("observedClusterDensity", value));
                }
            }

            sample.setAttributes(attributeSet);
            pipelineBeanService.getMaPSeqDAOBean().getHTSFSampleDAO().save(sample);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

    }

    private void saveDemulitplexStatsAttributes(File sequencerRunDir, String flowcell, HTSFSample sample) {

        File unalignedDir = new File(sequencerRunDir, String.format("Unaligned.%d", sample.getLaneIndex()));
        File baseCallStatsDir = new File(unalignedDir, String.format("Basecall_Stats_%s", flowcell));
        File statsFile = new File(baseCallStatsDir, "Demultiplex_Stats.htm");
        if (statsFile.exists()) {

            try {
                org.jsoup.nodes.Document doc = Jsoup.parse(FileUtils.readFileToString(statsFile));
                Iterator<Element> tableIter = doc.select("table").iterator();
                tableIter.next();

                for (Element row : tableIter.next().select("tr")) {

                    Iterator<Element> tdIter = row.select("td").iterator();

                    Element laneElement = tdIter.next();
                    Element sampleIdElement = tdIter.next();
                    Element sampleRefElement = tdIter.next();
                    Element indexElement = tdIter.next();
                    Element descriptionElement = tdIter.next();
                    Element controlElement = tdIter.next();
                    Element projectElement = tdIter.next();
                    Element yeildElement = tdIter.next();
                    Element passingFilteringElement = tdIter.next();
                    Element numberOfReadsElement = tdIter.next();
                    Element rawClustersPerLaneElement = tdIter.next();
                    Element perfectIndexReadsElement = tdIter.next();
                    Element oneMismatchReadsIndexElement = tdIter.next();
                    Element q30YeildPassingFilteringElement = tdIter.next();
                    Element meanQualityScorePassingFilteringElement = tdIter.next();

                    if (sample.getName().equals(sampleIdElement.text())
                            && sample.getLaneIndex().toString().equals(laneElement.text())
                            && sample.getBarcode().equals(indexElement.text())) {

                        Set<EntityAttribute> attributeSet = sample.getAttributes();

                        if (attributeSet == null) {
                            attributeSet = new HashSet<EntityAttribute>();
                        }

                        Set<String> entityAttributeNameSet = new HashSet<String>();

                        for (EntityAttribute attribute : attributeSet) {
                            entityAttributeNameSet.add(attribute.getName());
                        }

                        Set<String> synchSet = Collections.synchronizedSet(entityAttributeNameSet);

                        if (StringUtils.isNotEmpty(yeildElement.text())) {
                            String value = yeildElement.text().replace(",", "");
                            if (synchSet.contains("yield")) {
                                for (EntityAttribute attribute : attributeSet) {
                                    if (attribute.getName().equals("yield")) {
                                        attribute.setValue(value);
                                        break;
                                    }
                                }
                            } else {
                                attributeSet.add(new EntityAttribute("yield", value));
                            }
                        }

                        if (StringUtils.isNotEmpty(passingFilteringElement.text())) {
                            String value = passingFilteringElement.text();
                            if (synchSet.contains("passedFiltering")) {
                                for (EntityAttribute attribute : attributeSet) {
                                    if (attribute.getName().equals("passedFiltering")) {
                                        attribute.setValue(value);
                                        break;
                                    }
                                }
                            } else {
                                attributeSet.add(new EntityAttribute("passedFiltering", value));
                            }
                        }

                        if (StringUtils.isNotEmpty(numberOfReadsElement.text())) {
                            String value = numberOfReadsElement.text().replace(",", "");
                            if (synchSet.contains("numberOfReads")) {
                                for (EntityAttribute attribute : attributeSet) {
                                    if (attribute.getName().equals("numberOfReads")) {
                                        attribute.setValue(value);
                                        break;
                                    }
                                }
                            } else {
                                attributeSet.add(new EntityAttribute("numberOfReads", value));
                            }
                        }

                        if (StringUtils.isNotEmpty(rawClustersPerLaneElement.text())) {
                            String value = rawClustersPerLaneElement.text();
                            if (synchSet.contains("rawClustersPerLane")) {
                                for (EntityAttribute attribute : attributeSet) {
                                    if (attribute.getName().equals("rawClustersPerLane")) {
                                        attribute.setValue(value);
                                        break;
                                    }
                                }
                            } else {
                                attributeSet.add(new EntityAttribute("rawClustersPerLane", value));
                            }
                        }

                        if (StringUtils.isNotEmpty(perfectIndexReadsElement.text())) {
                            String value = perfectIndexReadsElement.text();
                            if (synchSet.contains("perfectIndexReads")) {
                                for (EntityAttribute attribute : attributeSet) {
                                    if (attribute.getName().equals("perfectIndexReads")) {
                                        attribute.setValue(value);
                                        break;
                                    }
                                }
                            } else {
                                attributeSet.add(new EntityAttribute("perfectIndexReads", value));
                            }
                        }

                        if (StringUtils.isNotEmpty(oneMismatchReadsIndexElement.text())) {
                            String value = oneMismatchReadsIndexElement.text();
                            if (synchSet.contains("oneMismatchReadsIndex")) {
                                for (EntityAttribute attribute : attributeSet) {
                                    if (attribute.getName().equals("oneMismatchReadsIndex")) {
                                        attribute.setValue(value);
                                        break;
                                    }
                                }
                            } else {
                                attributeSet.add(new EntityAttribute("oneMismatchReadsIndex", value));
                            }
                        }

                        if (StringUtils.isNotEmpty(q30YeildPassingFilteringElement.text())) {
                            String value = q30YeildPassingFilteringElement.text();
                            if (synchSet.contains("q30YieldPassingFiltering")) {
                                for (EntityAttribute attribute : attributeSet) {
                                    if (attribute.getName().equals("q30YieldPassingFiltering")) {
                                        attribute.setValue(value);
                                        break;
                                    }
                                }
                            } else {
                                attributeSet.add(new EntityAttribute("q30YieldPassingFiltering", value));
                            }
                        }

                        if (StringUtils.isNotEmpty(meanQualityScorePassingFilteringElement.text())) {
                            String value = meanQualityScorePassingFilteringElement.text();
                            if (synchSet.contains("meanQualityScorePassingFiltering")) {
                                for (EntityAttribute attribute : attributeSet) {
                                    if (attribute.getName().equals("meanQualityScorePassingFiltering")) {
                                        attribute.setValue(value);
                                        break;
                                    }
                                }
                            } else {
                                attributeSet.add(new EntityAttribute("meanQualityScorePassingFiltering", value));
                            }
                        }

                        sample.setAttributes(attributeSet);
                        pipelineBeanService.getMaPSeqDAOBean().getHTSFSampleDAO().save(sample);

                    }

                }
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
            }
        }
    }

    private String lookupFlowcell(File sequencerRunDir) {
        String flowcell = null;
        try {
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            File runInfoXmlFile = new File(sequencerRunDir, "RunInfo.xml");
            if (!runInfoXmlFile.exists()) {
                logger.error("RunInfo.xml file does not exist: {}", runInfoXmlFile.getAbsolutePath());
                return null;
            }
            FileInputStream fis = new FileInputStream(runInfoXmlFile);
            InputSource inputSource = new InputSource(fis);
            Document document = documentBuilder.parse(inputSource);
            XPath xpath = XPathFactory.newInstance().newXPath();

            // find the flowcell
            String runFlowcellIdPath = "/RunInfo/Run/Flowcell";
            Node runFlowcellIdNode = (Node) xpath.evaluate(runFlowcellIdPath, document, XPathConstants.NODE);
            flowcell = runFlowcellIdNode.getTextContent();
            logger.debug("flowcell = {}", flowcell);

        } catch (XPathExpressionException | DOMException | ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
        return flowcell;
    }

    public CASAVAPipelineBeanService getPipelineBeanService() {
        return pipelineBeanService;
    }

    public void setPipelineBeanService(CASAVAPipelineBeanService pipelineBeanService) {
        this.pipelineBeanService = pipelineBeanService;
    }

}
