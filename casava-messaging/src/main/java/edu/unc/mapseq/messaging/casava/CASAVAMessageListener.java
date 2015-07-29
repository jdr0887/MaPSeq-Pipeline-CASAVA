package edu.unc.mapseq.messaging.casava;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.unc.mapseq.dao.AttributeDAO;
import edu.unc.mapseq.dao.FileDataDAO;
import edu.unc.mapseq.dao.FlowcellDAO;
import edu.unc.mapseq.dao.JobDAO;
import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.StudyDAO;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.WorkflowRunAttemptDAO;
import edu.unc.mapseq.dao.WorkflowRunDAO;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Job;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Study;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.dao.model.WorkflowRunAttemptStatusType;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.impl.AbstractMessageListener;
import edu.unc.mapseq.workflow.model.WorkflowEntity;
import edu.unc.mapseq.workflow.model.WorkflowMessage;

public class CASAVAMessageListener extends AbstractMessageListener {

    private final Logger logger = LoggerFactory.getLogger(CASAVAMessageListener.class);

    public CASAVAMessageListener() {
        super();
    }

    @Override
    public void onMessage(Message message) {
        logger.debug("ENTERING onMessage(Message)");

        String messageValue = null;

        try {
            if (message instanceof TextMessage) {
                logger.debug("received TextMessage");
                TextMessage textMessage = (TextMessage) message;
                messageValue = textMessage.getText();
            }
        } catch (JMSException e2) {
            e2.printStackTrace();
        }

        if (StringUtils.isEmpty(messageValue)) {
            logger.warn("message value is empty");
            return;
        }

        logger.info("messageValue: {}", messageValue);

        ObjectMapper mapper = new ObjectMapper();
        WorkflowMessage workflowMessage = null;

        try {
            workflowMessage = mapper.readValue(messageValue, WorkflowMessage.class);
            if (workflowMessage.getEntities() == null) {
                logger.error("json lacks entities");
                return;
            }
        } catch (IOException e) {
            logger.error("BAD JSON format", e);
            return;
        }

        MaPSeqDAOBean daoBean = getWorkflowBeanService().getMaPSeqDAOBean();

        JobDAO jobDAO = daoBean.getJobDAO();
        FlowcellDAO flowcellDAO = daoBean.getFlowcellDAO();
        SampleDAO sampleDAO = daoBean.getSampleDAO();
        WorkflowDAO workflowDAO = daoBean.getWorkflowDAO();
        WorkflowRunDAO workflowRunDAO = daoBean.getWorkflowRunDAO();
        WorkflowRunAttemptDAO workflowRunAttemptDAO = daoBean.getWorkflowRunAttemptDAO();
        FileDataDAO fileDataDAO = daoBean.getFileDataDAO();
        StudyDAO studyDAO = daoBean.getStudyDAO();
        AttributeDAO attributeDAO = daoBean.getAttributeDAO();

        Flowcell flowcell = null;
        WorkflowRun workflowRun = null;

        File sampleSheet = null;

        Workflow workflow = null;
        try {
            List<Workflow> workflowList = workflowDAO.findByName(getWorkflowName());
            if (workflowList == null || (workflowList != null && workflowList.isEmpty())) {
                logger.error("No Workflow Found: {}", getWorkflowName());
                return;
            }
            workflow = workflowList.get(0);
        } catch (MaPSeqDAOException e) {
            logger.error("ERROR", e);
        }

        try {

            for (WorkflowEntity entity : workflowMessage.getEntities()) {
                if (StringUtils.isNotEmpty(entity.getEntityType())
                        && Flowcell.class.getSimpleName().equals(entity.getEntityType())) {
                    flowcell = getFlowcell(entity);
                }
            }

            for (WorkflowEntity entity : workflowMessage.getEntities()) {
                if (StringUtils.isNotEmpty(entity.getEntityType())
                        && WorkflowRun.class.getSimpleName().equals(entity.getEntityType())) {
                    workflowRun = getWorkflowRun(workflow, entity);
                }
            }

            for (WorkflowEntity entity : workflowMessage.getEntities()) {

                if (StringUtils.isNotEmpty(entity.getEntityType())
                        && FileData.class.getSimpleName().equals(entity.getEntityType())) {

                    Long id = entity.getId();
                    logger.debug("id: {}", id);
                    try {
                        FileData fileData = null;
                        try {
                            fileData = fileDataDAO.findById(id);
                        } catch (MaPSeqDAOException e) {
                            logger.error("ERROR", e);
                        }

                        if (fileData != null && fileData.getName().endsWith(".csv")
                                && fileData.getMimeType().equals(MimeType.TEXT_CSV)) {

                            logger.debug("fileData.toString(): {}", fileData.toString());

                            sampleSheet = new File(fileData.getPath(), fileData.getName());
                            String sampleSheetContent = FileUtils.readFileToString(sampleSheet);
                            Set<String> sampleProjectCache = findStudyName(sampleSheetContent);
                            Map<String, Study> studyMap = new HashMap<String, Study>();

                            for (String sampleProject : sampleProjectCache) {
                                try {
                                    List<Study> studyList = daoBean.getStudyDAO().findByName(sampleProject);
                                    Study study = studyList.get(0);
                                    if (study == null) {
                                        study = new Study();
                                        study.setName(sampleProject);
                                        Long studyId = studyDAO.save(study);
                                        study.setId(studyId);
                                    }
                                    studyMap.put(sampleProject, study);
                                } catch (Exception e) {
                                    logger.error("ERROR", e);
                                }
                            }

                            // flowcell base directory is derived from study (aka sampleProject)
                            // assume studyMap is size 1

                            if (studyMap.size() > 1) {
                                logger.error("Too many studies specified");
                                return;
                            }

                            String flowcellName = fileData.getName().replace(".csv", "");

                            File studyDirectory = new File(System.getenv("MAPSEQ_BASE_DIRECTORY"), studyMap.get(
                                    studyMap.keySet().iterator().next()).getName());
                            File baseDirectory = new File(studyDirectory, "out");
                            File flowcellDirectory = new File(baseDirectory, flowcellName);

                            Set<Integer> laneIndexSet = new HashSet<Integer>();

                            logger.debug("flowcellDirectory.exists(): {}", flowcellDirectory.exists());
                            if (!flowcellDirectory.exists()) {
                                logger.warn("expected flowcell directory does not exist: {}",
                                        flowcellDirectory.getAbsolutePath());
                                return;
                            }

                            flowcell = new Flowcell();
                            flowcell.setBaseDirectory(baseDirectory.getAbsolutePath());
                            flowcell.setName(flowcellName);

                            try {

                                List<Flowcell> foundFlowcells = flowcellDAO.findByExample(flowcell);

                                if (foundFlowcells != null && !foundFlowcells.isEmpty()) {

                                    flowcell = foundFlowcells.get(0);
                                    logger.info(flowcell.toString());

                                    List<Sample> samples = sampleDAO.findByFlowcellId(flowcell.getId());

                                    for (Sample sample : samples) {

                                        logger.info(sample.toString());

                                        List<WorkflowRun> workflowRuns = workflowRunDAO.findBySampleId(sample.getId());

                                        if (workflowRuns != null && !workflowRuns.isEmpty()) {

                                            for (WorkflowRun wr : workflowRuns) {

                                                logger.info(wr.toString());

                                                List<WorkflowRunAttempt> attempts = workflowRunAttemptDAO
                                                        .findByWorkflowRunId(wr.getId());

                                                if (attempts != null && !attempts.isEmpty()) {

                                                    for (WorkflowRunAttempt attempt : attempts) {
                                                        logger.info(attempt.toString());
                                                        List<Job> jobs = jobDAO.findByWorkflowRunAttemptId(attempt
                                                                .getId());

                                                        if (jobs != null && !jobs.isEmpty()) {
                                                            for (Job job : jobs) {
                                                                logger.info(job.toString());
                                                                job.setAttributes(null);
                                                                job.setFileDatas(null);
                                                                jobDAO.save(job);
                                                            }
                                                            jobDAO.delete(jobs);
                                                        }
                                                    }
                                                    workflowRunAttemptDAO.delete(attempts);

                                                }

                                            }
                                            workflowRunDAO.delete(workflowRuns);

                                        }

                                        sample.setAttributes(null);
                                        sample.setFileDatas(null);
                                        sampleDAO.save(sample);

                                    }
                                    sampleDAO.delete(samples);

                                } else {
                                    Long flowcellId = flowcellDAO.save(flowcell);
                                    flowcell.setId(flowcellId);
                                    logger.debug(flowcell.toString());
                                }
                            } catch (MaPSeqDAOException e) {
                                logger.error("Error", e);
                            }

                            if (flowcell == null) {
                                logger.warn("Invalid JSON: flowcell is null, not running anything");
                                return;
                            }

                            LineNumberReader lnr = new LineNumberReader(new StringReader(sampleSheetContent));
                            lnr.readLine();
                            String line;

                            while ((line = lnr.readLine()) != null) {

                                String[] st = line.split(",");
                                String flowcellProper = st[0];
                                String laneIndex = st[1];
                                laneIndexSet.add(Integer.valueOf(laneIndex));
                                String sampleId = st[2];
                                String sampleRef = st[3];
                                String index = st[4];
                                String description = st[5];
                                String control = st[6];
                                String recipe = st[7];
                                String operator = st[8];
                                String sampleProject = st[9];

                                try {
                                    Sample sample = new Sample();
                                    sample.setBarcode(index);
                                    sample.setLaneIndex(Integer.valueOf(laneIndex));
                                    sample.setName(sampleId);
                                    sample.setFlowcell(flowcell);
                                    sample.setStudy(studyMap.get(sampleProject));

                                    if (StringUtils.isNotEmpty(description)) {
                                        sample.getAttributes().add(
                                                new Attribute("production.id.description", description));
                                    }

                                    sampleDAO.save(sample);

                                } catch (MaPSeqDAOException e) {
                                    logger.error("ERROR", e);
                                }

                            }

                            Collections.synchronizedSet(laneIndexSet);
                            for (Integer lane : laneIndexSet) {
                                try {
                                    Sample sample = new Sample();
                                    sample.setBarcode("Undetermined");
                                    sample.setLaneIndex(lane);
                                    sample.setName(String.format("lane%d", lane));
                                    sample.setFlowcell(flowcell);
                                    sample.setStudy(studyMap.entrySet().iterator().next().getValue());
                                    sampleDAO.save(sample);
                                } catch (MaPSeqDAOException e) {
                                    logger.error("ERROR", e);
                                }
                            }

                        }
                    } catch (NumberFormatException | IOException e) {
                        logger.error("ERROR", e);
                    }

                }

            }

        } catch (WorkflowException e1) {
            logger.error(e1.getMessage(), e1);
            return;
        }

        WorkflowRunAttemptStatusType status = WorkflowRunAttemptStatusType.PENDING;

        if (workflowRun == null) {
            logger.warn("workflowRun is null, not running anything");
            return;
        }

        if (flowcell == null) {
            logger.warn("flowcell is null, not running anything");
            return;
        }

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

            readCount = 0;
            String readsPath = "/RunInfo/Run/Reads/Read/@IsIndexedRead";
            NodeList readsNodeList = (NodeList) xpath.evaluate(readsPath, document, XPathConstants.NODESET);
            for (int index = 0; index < readsNodeList.getLength(); index++) {
                if ("N".equals(readsNodeList.item(index).getTextContent())) {
                    ++readCount;
                }
            }
            logger.debug("readCount = {}", readCount);
            flowcell.getAttributes().add(new Attribute("readCount", readCount + ""));
            flowcell.getAttributes().add(new Attribute("sampleSheet", sampleSheet.getAbsolutePath()));
            flowcellDAO.save(flowcell);

            Set<Flowcell> flowcellSet = new HashSet<Flowcell>();
            flowcellSet.add(flowcell);
            workflowRun.setFlowcells(flowcellSet);

            Long workflowRunId = workflowRunDAO.save(workflowRun);
            workflowRun.setId(workflowRunId);

        } catch (XPathExpressionException | DOMException | ParserConfigurationException | SAXException
                | MaPSeqDAOException | IOException e) {
            status = WorkflowRunAttemptStatusType.FAILED;
            logger.warn("Error", e);
        }

        try {
            WorkflowRunAttempt attempt = new WorkflowRunAttempt();
            attempt.setStatus(status);
            attempt.setWorkflowRun(workflowRun);
            workflowRunAttemptDAO.save(attempt);
        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }
    }

    private Set<String> findStudyName(String sampleSheetContent) throws IOException {
        logger.info("ENTERING findStudyName(String)");
        Set<String> sampleProjectCache = new HashSet<String>();

        LineNumberReader lnr = new LineNumberReader(new StringReader(sampleSheetContent));
        lnr.readLine();
        String line;

        while ((line = lnr.readLine()) != null) {
            String[] st = line.split(",");
            String sampleProject = st[9];
            sampleProjectCache.add(sampleProject);
        }
        lnr.close();

        Collections.synchronizedSet(sampleProjectCache);
        return sampleProjectCache;
    }

}
