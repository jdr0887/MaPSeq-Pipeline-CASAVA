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
import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.Account;
import edu.unc.mapseq.dao.model.EntityAttribute;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.dao.model.Study;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowPlan;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunStatusType;
import edu.unc.mapseq.dao.ws.WSDAOManager;
import edu.unc.mapseq.workflow.WorkflowBeanService;
import edu.unc.mapseq.workflow.WorkflowExecutor;
import edu.unc.mapseq.workflow.casava.CASAVAWorkflow;
import edu.unc.mapseq.workflow.impl.WorkflowBeanServiceImpl;

public class RunWorkflow implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(RunWorkflow.class);

    private final static HelpFormatter helpFormatter = new HelpFormatter();

    private final static Options cliOptions = new Options();

    private final WSDAOManager daoMgr = WSDAOManager.getInstance();

    private String workflowRunName;

    private Long sequencerRunId;

    private Long htsfSampleId;

    private Properties properties;

    public RunWorkflow() {
        super();
    }

    @Override
    public void run() {

        MaPSeqDAOBean maPSeqDAOBean = daoMgr.getMaPSeqDAOBean();

        Account account = null;
        try {
            List<Account> accountList = maPSeqDAOBean.getAccountDAO().findByName(System.getProperty("user.name"));
            if (accountList == null || (accountList != null && accountList.isEmpty())) {
                System.err.printf("Account doesn't exist: %s%n", System.getProperty("user.name"));
                System.err.println("Must register account first");
                return;
            }
            account = accountList.get(0);
        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

        if (this.sequencerRunId == null && this.htsfSampleId == null) {
            System.err.println("Both sequencerRunId and htsfSampeId can't be null");
            return;
        }

        SequencerRun sequencerRun = null;
        Set<HTSFSample> htsfSampleSet = new HashSet<HTSFSample>();

        try {
            if (sequencerRunId != null && htsfSampleId == null) {
                sequencerRun = daoMgr.getMaPSeqDAOBean().getSequencerRunDAO().findById(this.sequencerRunId);
            } else if (sequencerRunId == null && htsfSampleId != null) {
                HTSFSample htsfSample = daoMgr.getMaPSeqDAOBean().getHTSFSampleDAO().findById(this.htsfSampleId);
                htsfSampleSet.add(htsfSample);
            }
        } catch (MaPSeqDAOException e) {
        }

        if (sequencerRun == null && htsfSampleSet.size() == 0) {
            System.err.println("SequencerRun & Set<HTSFSample> are both empty");
            return;
        }

        CASAVAWorkflow casavaWorkflow = new CASAVAWorkflow();
        Workflow workflow = null;
        try {
            List<Workflow> workflowList = daoMgr.getMaPSeqDAOBean().getWorkflowDAO().findByName("CASAVA");
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
        workflowRun.setCreator(account);
        workflowRun.setName(this.workflowRunName);
        workflowRun.setWorkflow(workflow);
        workflowRun.setVersion(casavaWorkflow.getVersion());

        try {
            workflowRun.setStatus(WorkflowRunStatusType.PENDING);
            Long workflowRunId = daoMgr.getMaPSeqDAOBean().getWorkflowRunDAO().save(workflowRun);
            workflowRun.setId(workflowRunId);
        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

        try {
            WorkflowPlan workflowPlan = new WorkflowPlan();
            if (sequencerRun != null) {
                workflowPlan.setSequencerRun(sequencerRun);
            }
            if (htsfSampleSet.size() > 0) {
                workflowPlan.setHTSFSamples(htsfSampleSet);
            }
            workflowPlan.setWorkflowRun(workflowRun);
            Long workflowPlanId = daoMgr.getMaPSeqDAOBean().getWorkflowPlanDAO().save(workflowPlan);
            workflowPlan.setId(workflowPlanId);

            MaPSeqConfigurationService configService = new MaPSeqConfigurationServiceImpl();
            System.out.println("Please watch " + System.getenv("MAPSEQ_HOME")
                    + "/logs/mapseq.log for state changes and/or progress");
            WorkflowBeanService workflowBeanService = new WorkflowBeanServiceImpl();

            Map<String, String> attributeMap = new HashMap<String, String>();
            attributeMap.put("siteName", "Kure");
            workflowBeanService.setAttributes(attributeMap);
            workflowBeanService.setMaPSeqConfigurationService(configService);
            workflowBeanService.setMaPSeqDAOBean(daoMgr.getMaPSeqDAOBean());

            File baseDir = new File(sequencerRun.getBaseDirectory());
            File sequencerRunDir = new File(baseDir, sequencerRun.getName());
            File dataDir = new File(sequencerRunDir, "Data");
            File intensitiesDir = new File(dataDir, "Intensities");
            File baseCallsDir = new File(intensitiesDir, "BaseCalls");

            logger.debug("baseCallsDir.getAbsolutePath() = {}", baseCallsDir.getAbsolutePath());

            if (!baseCallsDir.exists()) {
                logger.error("baseCallsDir does not exist");
                return;
            }

            String flowcell = null;

            Set<EntityAttribute> entityAttributeSet = sequencerRun.getAttributes();
            if (entityAttributeSet == null) {
                entityAttributeSet = new HashSet<EntityAttribute>();
            }

            int readCount = 1;
            try {
                DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                File runInfoXmlFile = new File(sequencerRunDir, "RunInfo.xml");
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
                flowcell = runFlowcellIdNode.getTextContent();
                logger.debug("flowcell = {}", flowcell);

                readCount = 0;
                String readsPath = "/RunInfo/Run/Reads/Read/@IsIndexedRead";
                NodeList readsNodeList = (NodeList) xpath.evaluate(readsPath, document, XPathConstants.NODESET);
                for (int index = 0; index < readsNodeList.getLength(); index++) {
                    if ("N".equals(readsNodeList.item(index).getTextContent())) {
                        ++readCount;
                    }
                }
                logger.debug("readCount = {}", readCount);
                entityAttributeSet.add(new EntityAttribute("readCount", readCount + ""));
            } catch (XPathExpressionException | DOMException | ParserConfigurationException | SAXException
                    | IOException e1) {
                e1.printStackTrace();
            }

            Vector<Vector<String>> data = new Vector<Vector<String>>();
            try {
                List<HTSFSample> htsfSampleList = daoMgr.getMaPSeqDAOBean().getHTSFSampleDAO()
                        .findBySequencerRunId(sequencerRun.getId());

                if (htsfSampleList != null && htsfSampleList.size() > 0) {
                    logger.debug("htsfSampleList.size() = {}", htsfSampleList.size());
                    for (HTSFSample sample : htsfSampleList) {
                        Study study = sample.getStudy();
                        Vector<String> childVector = new Vector<String>();
                        childVector.add(sample.getLaneIndex().toString());
                        childVector.add(sample.getName());
                        childVector.add(sample.getBarcode());
                        childVector.add(study.getName());
                        data.add(childVector);
                    }
                }
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
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
                sb.append(String.format("%s,%s,%s,,%s,,,,,%s%n", flowcell, values.get(0), values.get(1), values.get(2),
                        values.get(3)));
            }

            File sampleSheetFile = new File(baseCallsDir, "SampleSheet.csv");
            try {
                FileUtils.write(sampleSheetFile, sb.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }

            entityAttributeSet.add(new EntityAttribute("sampleSheet", sampleSheetFile.getAbsolutePath()));

            sequencerRun.setAttributes(entityAttributeSet);
            daoMgr.getMaPSeqDAOBean().getSequencerRunDAO().save(sequencerRun);

            casavaWorkflow.setWorkflowBeanService(workflowBeanService);
            casavaWorkflow.setWorkflowPlan(workflowPlan);
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

    public Long getSequencerRunId() {
        return sequencerRunId;
    }

    public void setSequencerRunId(Long sequencerRunId) {
        this.sequencerRunId = sequencerRunId;
    }

    public Long getHtsfSampleId() {
        return htsfSampleId;
    }

    public void setHtsfSampleId(Long htsfSampleId) {
        this.htsfSampleId = htsfSampleId;
    }

    @SuppressWarnings("static-access")
    public static void main(String[] args) {
        cliOptions.addOption(OptionBuilder.withArgName("htsfSampleId").hasArg()
                .withDescription("HTSFSample identifier").withLongOpt("htsfSampleId").create());
        cliOptions.addOption(OptionBuilder.withArgName("sequencerRunId").hasArg()
                .withDescription("SequencerRun identifier").withLongOpt("sequencerRunId").create());
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

            if (commandLine.hasOption("sequencerRunId")) {
                Long sequencerRunId = Long.valueOf(commandLine.getOptionValue("sequencerRunId"));
                main.setSequencerRunId(sequencerRunId);
            }

            if (commandLine.hasOption("htsfSampleId")) {
                Long htsfSampleId = Long.valueOf(commandLine.getOptionValue("htsfSampleId"));
                main.setHtsfSampleId(htsfSampleId);
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
