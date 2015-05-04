package edu.unc.mapseq.casava;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Executors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import edu.unc.mapseq.config.MaPSeqConfigurationService;
import edu.unc.mapseq.config.MaPSeqConfigurationServiceImpl;
import edu.unc.mapseq.dao.AttributeDAO;
import edu.unc.mapseq.dao.FlowcellDAO;
import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.WorkflowRunAttemptDAO;
import edu.unc.mapseq.dao.WorkflowRunDAO;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Study;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.dao.model.WorkflowRunAttemptStatusType;
import edu.unc.mapseq.dao.ws.WSDAOManager;
import edu.unc.mapseq.workflow.WorkflowBeanService;
import edu.unc.mapseq.workflow.WorkflowExecutor;
import edu.unc.mapseq.workflow.casava.CASAVAWorkflow;
import edu.unc.mapseq.workflow.impl.WorkflowBeanServiceImpl;

public class RunWorkflow implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(RunWorkflow.class);

    private final static HelpFormatter helpFormatter = new HelpFormatter();

    private final static Options cliOptions = new Options();

    private String workflowRunName;

    private Long flowcellId;

    private Long sampleId;

    private Properties properties;

    public RunWorkflow() {
        super();
    }

    @Override
    public void run() {

        WSDAOManager daoMgr = WSDAOManager.getInstance();
        MaPSeqDAOBean maPSeqDAOBean = daoMgr.getMaPSeqDAOBean();

        AttributeDAO attributeDAO = maPSeqDAOBean.getAttributeDAO();
        FlowcellDAO flowcellDAO = maPSeqDAOBean.getFlowcellDAO();
        SampleDAO sampleDAO = maPSeqDAOBean.getSampleDAO();
        WorkflowDAO workflowDAO = maPSeqDAOBean.getWorkflowDAO();
        WorkflowRunDAO workflowRunDAO = maPSeqDAOBean.getWorkflowRunDAO();
        WorkflowRunAttemptDAO workflowRunAttemptDAO = maPSeqDAOBean.getWorkflowRunAttemptDAO();

        if (this.flowcellId == null && this.sampleId == null) {
            System.err.println("Both flowcellId and sampleId can't be null");
            return;
        }

        Flowcell flowcell = null;
        Set<Sample> sampleSet = new HashSet<Sample>();

        try {
            if (flowcellId != null && sampleId == null) {
                flowcell = flowcellDAO.findById(this.flowcellId);
            } else if (flowcellId == null && sampleId != null) {
                Sample sample = sampleDAO.findById(this.sampleId);
                sampleSet.add(sample);
            }
        } catch (MaPSeqDAOException e) {
        }

        if (flowcell == null && sampleSet.isEmpty()) {
            System.err.println("Flowcell & Set<Sample> are both empty");
            return;
        }

        CASAVAWorkflow casavaWorkflow = new CASAVAWorkflow();
        Workflow workflow = null;
        try {
            List<Workflow> workflowList = workflowDAO.findByName("CASAVA");
            if (workflowList != null && !workflowList.isEmpty()) {
                workflow = workflowList.get(0);
            }
        } catch (MaPSeqDAOException e2) {
            e2.printStackTrace();
        }

        if (workflow == null) {
            System.err.println("Workflow doesn't exist");
            return;
        }

        WorkflowRun workflowRun = new WorkflowRun();
        workflowRun.setName(this.workflowRunName);
        workflowRun.setWorkflow(workflow);
        if (flowcell != null) {
            Set<Flowcell> flowcellSet = workflowRun.getFlowcells();
            if (flowcellSet == null) {
                flowcellSet = new HashSet<Flowcell>();
            }
            flowcellSet.add(flowcell);
            workflowRun.setFlowcells(flowcellSet);
        }
        if (!sampleSet.isEmpty()) {
            workflowRun.setSamples(sampleSet);
        }

        try {
            Long workflowRunId = workflowRunDAO.save(workflowRun);
            workflowRun.setId(workflowRunId);
        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

        try {
            WorkflowRunAttempt attempt = new WorkflowRunAttempt();
            attempt.setVersion(casavaWorkflow.getVersion());
            attempt.setStatus(WorkflowRunAttemptStatusType.PENDING);
            attempt.setWorkflowRun(workflowRun);

            Long attemptId = workflowRunAttemptDAO.save(attempt);
            attempt.setId(attemptId);

            MaPSeqConfigurationService configService = new MaPSeqConfigurationServiceImpl();
            System.out.println("Please watch " + System.getenv("MAPSEQ_HOME")
                    + "/logs/mapseq.log for state changes and/or progress");
            WorkflowBeanService workflowBeanService = new WorkflowBeanServiceImpl();

            Map<String, String> attributeMap = new HashMap<String, String>();
            attributeMap.put("siteName", "Kure");
            workflowBeanService.setAttributes(attributeMap);
            workflowBeanService.setMaPSeqConfigurationService(configService);
            workflowBeanService.setMaPSeqDAOBean(daoMgr.getMaPSeqDAOBean());

            File baseDir = new File(flowcell.getBaseDirectory());
            File flowcellDir = new File(baseDir, flowcell.getName());
            File dataDir = new File(flowcellDir, "Data");
            File intensitiesDir = new File(dataDir, "Intensities");
            File baseCallsDir = new File(intensitiesDir, "BaseCalls");

            logger.debug("baseCallsDir.getAbsolutePath() = {}", baseCallsDir.getAbsolutePath());

            if (!baseCallsDir.exists()) {
                logger.error("baseCallsDir does not exist");
                return;
            }

            String flowcellProper = null;

            int readCount = 1;
            try {
                DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                File runInfoXmlFile = new File(flowcellDir, "RunInfo.xml");
                if (!runInfoXmlFile.exists()) {
                    logger.error("RunInfo.xml file does not exist: {}", runInfoXmlFile.getAbsolutePath());
                    return;
                }
                FileInputStream fis = new FileInputStream(runInfoXmlFile);
                InputSource inputSource = new InputSource(fis);
                Document document = documentBuilder.parse(inputSource);
                XPath xpath = XPathFactory.newInstance().newXPath();

                // find the flowcell
                String runFlowcellIdPath = "/RunInfo/Run/Flowcell";
                Node runFlowcellIdNode = (Node) xpath.evaluate(runFlowcellIdPath, document, XPathConstants.NODE);
                flowcellProper = runFlowcellIdNode.getTextContent();
                logger.debug("flowcellProper = {}", flowcellProper);

                readCount = 0;
                String readsPath = "/RunInfo/Run/Reads/Read/@IsIndexedRead";
                NodeList readsNodeList = (NodeList) xpath.evaluate(readsPath, document, XPathConstants.NODESET);
                for (int index = 0; index < readsNodeList.getLength(); index++) {
                    if ("N".equals(readsNodeList.item(index).getTextContent())) {
                        ++readCount;
                    }
                }
                logger.debug("readCount = {}", readCount);

                Set<String> attributeNameSet = new HashSet<String>();

                Set<Attribute> attributes = flowcell.getAttributes();

                if (attributes != null && attributes.isEmpty()) {
                    for (Attribute attribute : attributes) {
                        attributeNameSet.add(attribute.getName());
                    }
                    Set<String> synchSet = Collections.synchronizedSet(attributeNameSet);
                    if (synchSet.contains("readCount")) {
                        for (Attribute attribute : attributes) {
                            if (attribute.getName().equals("readCount")) {
                                attribute.setValue(readCount + "");
                                attributeDAO.save(attribute);
                                break;
                            }
                        }
                    } else {
                        attributes.add(new Attribute("readCount", readCount + ""));
                    }
                }

                flowcell.setAttributes(attributes);

            } catch (XPathExpressionException | DOMException | ParserConfigurationException | SAXException
                    | IOException e1) {
                e1.printStackTrace();
            }

            Vector<Vector<String>> data = new Vector<Vector<String>>();
            List<Sample> sampleList = sampleDAO.findByFlowcellId(flowcell.getId());

            if (sampleList != null && !sampleList.isEmpty()) {
                logger.debug("sampleList.size() = {}", sampleList.size());
                for (Sample sample : sampleList) {
                    Study study = sample.getStudy();
                    Vector<String> childVector = new Vector<String>();
                    childVector.add(sample.getLaneIndex().toString());
                    childVector.add(sample.getName());
                    childVector.add(sample.getBarcode());
                    childVector.add(study.getName());
                    data.add(childVector);
                }
            }

            StringBuilder sb = new StringBuilder(
                    "FCID,Lane,SampleID,SampleRef,Index,Description,Control,Recipe,Operator,SampleProject\n");
            Collections.sort(data, new Comparator<Vector<String>>() {
                @Override
                public int compare(Vector<String> arg0, Vector<String> arg1) {
                    return Integer.valueOf(arg0.get(0)).compareTo(Integer.valueOf(arg1.get(0)));
                }
            });

            Iterator<Vector<String>> dataIter = data.iterator();
            while (dataIter.hasNext()) {
                Vector<String> values = dataIter.next();
                sb.append(String.format("%s,%s,%s,,%s,,,,,%s%n", flowcellProper, values.get(0), values.get(1),
                        values.get(2), values.get(3)));
            }

            File sampleSheetFile = new File(baseCallsDir, "SampleSheet.csv");
            try {
                FileUtils.write(sampleSheetFile, sb.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }

            Set<String> attributeNameSet = new HashSet<String>();

            Set<Attribute> attributes = flowcell.getAttributes();

            if (attributes != null && attributes.isEmpty()) {
                for (Attribute attribute : attributes) {
                    attributeNameSet.add(attribute.getName());
                }
                Set<String> synchSet = Collections.synchronizedSet(attributeNameSet);
                if (synchSet.contains("readCount")) {
                    for (Attribute attribute : attributes) {
                        if (attribute.getName().equals("sampleSheet")) {
                            attribute.setValue(sampleSheetFile.getAbsolutePath());
                            attributeDAO.save(attribute);
                            break;
                        }
                    }
                } else {
                    attributes.add(new Attribute("sampleSheet", sampleSheetFile.getAbsolutePath()));
                }
            }
            flowcell.setAttributes(attributes);

            flowcellDAO.save(flowcell);

            casavaWorkflow.setWorkflowBeanService(workflowBeanService);
            casavaWorkflow.setWorkflowRunAttempt(attempt);
            Executors.newSingleThreadExecutor().execute(new WorkflowExecutor(casavaWorkflow));

        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public String getWorkflowRunName() {
        return workflowRunName;
    }

    public void setWorkflowRunName(String workflowRunName) {
        this.workflowRunName = workflowRunName;
    }

    public Long getFlowcellId() {
        return flowcellId;
    }

    public void setFlowcellId(Long flowcellId) {
        this.flowcellId = flowcellId;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

    @SuppressWarnings("static-access")
    public static void main(String[] args) {
        cliOptions.addOption(OptionBuilder.withArgName("sampleId").hasArg().withDescription("Sample identifier")
                .withLongOpt("sampleId").create());
        cliOptions.addOption(OptionBuilder.withArgName("flowcellId").hasArg().withDescription("Flowcell identifier")
                .withLongOpt("flowcellId").create());
        cliOptions.addOption(OptionBuilder.withArgName("workflowRunName").withLongOpt("workflowRunName").isRequired()
                .hasArg().create());
        cliOptions.addOption(OptionBuilder.withArgName("propertyFile").withLongOpt("propertyFile").hasArg().create());

        RunWorkflow main = new RunWorkflow();
        CommandLineParser commandLineParser = new GnuParser();
        try {
            CommandLine commandLine = commandLineParser.parse(cliOptions, args);
            if (commandLine.hasOption("?")) {
                helpFormatter.printHelp(main.getClass().getSimpleName(), cliOptions);
                return;
            }

            if (commandLine.hasOption("workflowRunName")) {
                String workflowRunName = commandLine.getOptionValue("workflowRunName");
                main.setWorkflowRunName(workflowRunName);
            }

            if (commandLine.hasOption("flowcellId")) {
                Long flowcellId = Long.valueOf(commandLine.getOptionValue("flowcellId"));
                main.setFlowcellId(flowcellId);
            }

            if (commandLine.hasOption("sampleId")) {
                Long sampleId = Long.valueOf(commandLine.getOptionValue("sampleId"));
                main.setSampleId(sampleId);
            }

            if (commandLine.hasOption("propertyFile")) {
                Properties properties = new Properties();
                properties.load(new FileInputStream(new File(commandLine.getOptionValue("propertyFile"))));
                main.setProperties(properties);
            }

            main.run();
        } catch (ParseException e) {
            System.err.println(("Parsing Failed: " + e.getMessage()));
            helpFormatter.printHelp(main.getClass().getSimpleName(), cliOptions);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(("Error: " + e.getMessage()));
            helpFormatter.printHelp(main.getClass().getSimpleName(), cliOptions);
        }

    }

}
