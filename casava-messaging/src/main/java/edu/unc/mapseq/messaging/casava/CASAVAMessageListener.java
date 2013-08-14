package edu.unc.mapseq.messaging.casava;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
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
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import edu.unc.mapseq.dao.HTSFSampleDAO;
import edu.unc.mapseq.dao.JobDAO;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.WorkflowPlanDAO;
import edu.unc.mapseq.dao.WorkflowRunDAO;
import edu.unc.mapseq.dao.model.Account;
import edu.unc.mapseq.dao.model.EntityAttribute;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.Job;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Platform;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.dao.model.SequencerRunStatusType;
import edu.unc.mapseq.dao.model.Study;
import edu.unc.mapseq.dao.model.WorkflowPlan;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunStatusType;
import edu.unc.mapseq.pipeline.EntityUtil;
import edu.unc.mapseq.pipeline.PipelineBeanService;

public class CASAVAMessageListener implements MessageListener {

    private final Logger logger = LoggerFactory.getLogger(CASAVAMessageListener.class);

    private PipelineBeanService pipelineBeanService;

    public CASAVAMessageListener() {
        super();
    }

    public CASAVAMessageListener(PipelineBeanService pipelineBeanService) {
        super();
        this.pipelineBeanService = pipelineBeanService;
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

        JSONObject jsonMessage = null;

        try {
            jsonMessage = new JSONObject(messageValue);
            if (!jsonMessage.has("entities") || !jsonMessage.has("account_name")) {
                logger.error("json lacks entities or account_name");
                return;
            }
        } catch (JSONException e) {
            logger.error("BAD JSON format", e);
            return;
        }

        SequencerRun sequencerRun = null;
        WorkflowRun workflowRun = null;
        Platform platform = null;
        Account account = null;

        File sampleSheet = null;

        try {
            String accountName = jsonMessage.getString("account_name");
            account = pipelineBeanService.getMaPSeqDAOBean().getAccountDAO().findByName(accountName);
        } catch (JSONException | MaPSeqDAOException e) {
            logger.error("Error", e);
        } catch (Exception e) {
            logger.error("Error", e);
        }

        if (account == null) {
            logger.error("Must register account first");
            return;
        }

        try {
            JSONArray entityArray = jsonMessage.getJSONArray("entities");
            logger.debug("entityArray.length(): {}", entityArray.length());

            for (int i = 0; i < entityArray.length(); ++i) {

                JSONObject entityJSONObject = entityArray.getJSONObject(i);
                String entityType = entityJSONObject.getString("entity_type");

                logger.debug("entityType: {}", entityType);

                if ("Sequencer run".equals(entityType) || SequencerRun.class.getSimpleName().equals(entityType)) {
                    sequencerRun = EntityUtil.getSequencerRun(pipelineBeanService.getMaPSeqDAOBean(), entityJSONObject);
                }

                if (FileData.class.getSimpleName().equals(entityType)) {
                    Long guid = entityJSONObject.getLong("id");
                    logger.debug("guid: {}", guid);
                    try {
                        FileData fileData = null;
                        try {
                            fileData = pipelineBeanService.getMaPSeqDAOBean().getFileDataDAO().findById(guid);
                        } catch (MaPSeqDAOException e) {
                            logger.error("ERROR", e);
                        }

                        if (fileData != null && fileData.getName().endsWith(".csv")
                                && fileData.getMimeType().equals(MimeType.TEXT_CSV)) {

                            logger.debug("fileData.toString(): {}", fileData.toString());

                            Date creationDate = new Date();

                            sampleSheet = new File(fileData.getPath(), fileData.getName());

                            String sampleSheetContent = FileUtils.readFileToString(sampleSheet);

                            Set<String> sampleProjectCache = findStudyName(sampleSheetContent);

                            Map<String, Study> studyMap = new HashMap<String, Study>();

                            for (String sampleProject : sampleProjectCache) {
                                try {
                                    Study study = pipelineBeanService.getMaPSeqDAOBean().getStudyDAO()
                                            .findByName(sampleProject);
                                    if (study == null) {
                                        study = new Study();
                                        study.setCreationDate(creationDate);
                                        study.setModificationDate(creationDate);
                                        study.setCreator(account);
                                        study.setName(sampleProject);
                                        Long studyId = pipelineBeanService.getMaPSeqDAOBean().getStudyDAO().save(study);
                                        study.setId(studyId);
                                    }
                                    studyMap.put(sampleProject, study);
                                } catch (Exception e) {
                                    logger.error("ERROR", e);
                                }
                            }

                            // sequencerRun base directory is derived from study (aka sampleProject)
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

                            if (flowcellDirectory.exists()) {

                                sequencerRun = new SequencerRun();
                                sequencerRun.setBaseDirectory(baseDirectory.getAbsolutePath());
                                sequencerRun.setName(flowcellName);

                                try {

                                    List<SequencerRun> foundSequencerRuns = pipelineBeanService.getMaPSeqDAOBean()
                                            .getSequencerRunDAO().findByExample(sequencerRun);

                                    if (foundSequencerRuns != null && foundSequencerRuns.size() > 0) {

                                        sequencerRun = foundSequencerRuns.get(0);
                                        logger.debug("sequencerRun.toString(): {}", sequencerRun.toString());

                                        // sequencerRun to htsfSample is one to many and if a sequencerRun is found,
                                        // reset the htsfSamples from the samplesheet, but must first delete existing
                                        // htsfSamples
                                        JobDAO jobDAO = pipelineBeanService.getMaPSeqDAOBean().getJobDAO();

                                        HTSFSampleDAO htsfSampleDAO = pipelineBeanService.getMaPSeqDAOBean()
                                                .getHTSFSampleDAO();

                                        List<HTSFSample> htsfSamplesToDeleteList = htsfSampleDAO
                                                .findBySequencerRunId(sequencerRun.getId());

                                        List<Job> jobsToDeleteList = new ArrayList<Job>();
                                        List<WorkflowPlan> workflowPlansToDeleteList = new ArrayList<WorkflowPlan>();
                                        List<WorkflowRun> workflowRunsToDeleteList = new ArrayList<WorkflowRun>();

                                        WorkflowRunDAO workflowRunDAO = pipelineBeanService.getMaPSeqDAOBean()
                                                .getWorkflowRunDAO();

                                        WorkflowPlanDAO workflowPlanDAO = pipelineBeanService.getMaPSeqDAOBean()
                                                .getWorkflowPlanDAO();

                                        for (HTSFSample sample : htsfSamplesToDeleteList) {

                                            logger.debug("sample.toString(): {}", sample.toString());

                                            List<WorkflowPlan> workflowPlanList = workflowPlanDAO
                                                    .findByHTSFSampleId(sample.getId());

                                            if (workflowPlanList == null) {
                                                logger.warn("no WorkflowPlan instances found");
                                                continue;
                                            }

                                            for (WorkflowPlan workflowPlan : workflowPlanList) {
                                                WorkflowRun workflowRunToDelete = workflowPlan.getWorkflowRun();
                                                jobsToDeleteList.addAll(jobDAO.findByWorkflowRunId(workflowRunToDelete
                                                        .getId()));
                                                workflowPlan.setHTSFSamples(null);
                                                workflowPlanDAO.save(workflowPlan);
                                                workflowPlansToDeleteList.add(workflowPlan);
                                                workflowRunsToDeleteList.add(workflowRunToDelete);

                                            }

                                        }

                                        // this will take a long time if there are lots of downstream analysis
                                        jobDAO.delete(jobsToDeleteList);
                                        workflowPlanDAO.delete(workflowPlansToDeleteList);
                                        workflowRunDAO.delete(workflowRunsToDeleteList);
                                        htsfSampleDAO.delete(htsfSamplesToDeleteList);

                                    } else {
                                        sequencerRun.setCreator(account);
                                        sequencerRun.setStatus(SequencerRunStatusType.COMPLETED);
                                        sequencerRun.setCreationDate(creationDate);
                                        sequencerRun.setModificationDate(creationDate);
                                        Long sequencerRunId = pipelineBeanService.getMaPSeqDAOBean()
                                                .getSequencerRunDAO().save(sequencerRun);
                                        sequencerRun.setId(sequencerRunId);
                                        logger.debug("sequencerRun.toString(): {}", sequencerRun.toString());
                                    }
                                } catch (MaPSeqDAOException e) {
                                    logger.error("Error", e);
                                }
                            }

                            if (sequencerRun == null) {
                                logger.warn("Invalid JSON: sequencerRun is null, not running anything");
                                return;
                            }

                            LineNumberReader lnr = new LineNumberReader(new StringReader(sampleSheetContent));
                            lnr.readLine();
                            String line;

                            while ((line = lnr.readLine()) != null) {

                                String[] st = line.split(",");
                                String flowcell = st[0];
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
                                    HTSFSample htsfSample = new HTSFSample();
                                    htsfSample.setBarcode(index);
                                    htsfSample.setCreationDate(creationDate);
                                    htsfSample.setModificationDate(creationDate);
                                    htsfSample.setCreator(account);
                                    htsfSample.setLaneIndex(Integer.valueOf(laneIndex));
                                    htsfSample.setName(sampleId);
                                    htsfSample.setSequencerRun(sequencerRun);
                                    htsfSample.setStudy(studyMap.get(sampleProject));

                                    Set<EntityAttribute> attributes = htsfSample.getAttributes();
                                    if (attributes == null) {
                                        attributes = new HashSet<EntityAttribute>();
                                    }

                                    if (StringUtils.isNotEmpty(description)) {
                                        EntityAttribute descAttribute = new EntityAttribute();
                                        descAttribute.setName("production.id.description");
                                        descAttribute.setValue(description);
                                        attributes.add(descAttribute);
                                    }

                                    htsfSample.setAttributes(attributes);

                                    pipelineBeanService.getMaPSeqDAOBean().getHTSFSampleDAO().save(htsfSample);

                                } catch (MaPSeqDAOException e) {
                                    logger.error("ERROR", e);
                                }

                            }

                            Collections.synchronizedSet(laneIndexSet);
                            for (Integer lane : laneIndexSet) {
                                try {
                                    HTSFSample htsfSample = new HTSFSample();
                                    htsfSample.setBarcode("Undetermined");
                                    htsfSample.setCreationDate(creationDate);
                                    htsfSample.setModificationDate(creationDate);
                                    htsfSample.setCreator(account);
                                    htsfSample.setLaneIndex(lane);
                                    htsfSample.setName(String.format("lane%d", lane));
                                    htsfSample.setSequencerRun(sequencerRun);
                                    htsfSample.setStudy(studyMap.entrySet().iterator().next().getValue());
                                    pipelineBeanService.getMaPSeqDAOBean().getHTSFSampleDAO().save(htsfSample);
                                } catch (MaPSeqDAOException e) {
                                    logger.error("ERROR", e);
                                }
                            }

                        }
                    } catch (NumberFormatException | IOException e) {
                        logger.error("ERROR", e);
                    }
                }

                if ("Workflow run".equals(entityType) || WorkflowRun.class.getSimpleName().equals(entityType)) {
                    workflowRun = EntityUtil.getWorkflowRun(pipelineBeanService.getMaPSeqDAOBean(), "CASAVA",
                            entityJSONObject, account);
                    logger.debug("workflowRun.toString(): {}", workflowRun.toString());
                }

                if (Platform.class.getSimpleName().equals(entityType)) {
                    platform = EntityUtil.getPlatform(pipelineBeanService.getMaPSeqDAOBean(), entityJSONObject);
                    logger.debug("platform.toString(): {}", platform.toString());
                }

            }
        } catch (JSONException e) {
            logger.error("ERROR", e);
            return;
        }

        if (workflowRun == null) {
            logger.warn("workflowRun is null, not running anything");
            return;
        }

        if (sequencerRun == null) {
            logger.warn("sequencerRun is null, not running anything");
            workflowRun.setStatus(WorkflowRunStatusType.FAILED);
            return;
        }

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

            readCount = 0;
            String readsPath = "/RunInfo/Run/Reads/Read/@IsIndexedRead";
            NodeList readsNodeList = (NodeList) xpath.evaluate(readsPath, document, XPathConstants.NODESET);
            for (int index = 0; index < readsNodeList.getLength(); index++) {
                if ("N".equals(readsNodeList.item(index).getTextContent())) {
                    ++readCount;
                }
            }
            logger.debug("readCount = {}", readCount);
            Set<EntityAttribute> attributeSet = sequencerRun.getAttributes();
            if (attributeSet == null) {
                attributeSet = new HashSet<EntityAttribute>();
            }
            attributeSet.add(new EntityAttribute("readCount", readCount + ""));
            attributeSet.add(new EntityAttribute("sampleSheet", sampleSheet.getAbsolutePath()));
            if (platform == null) {
                // get default platform
                platform = pipelineBeanService.getMaPSeqDAOBean().getPlatformDAO().findById(66L);
            }
            sequencerRun.setPlatform(platform);
            sequencerRun.setAttributes(attributeSet);
            pipelineBeanService.getMaPSeqDAOBean().getSequencerRunDAO().save(sequencerRun);
            Long workflowRunId = pipelineBeanService.getMaPSeqDAOBean().getWorkflowRunDAO().save(workflowRun);
            workflowRun.setId(workflowRunId);

        } catch (XPathExpressionException | DOMException | ParserConfigurationException | SAXException
                | MaPSeqDAOException | IOException e) {
            workflowRun.setStatus(WorkflowRunStatusType.FAILED);
            e.printStackTrace();
        }

        try {
            WorkflowPlan workflowPlan = new WorkflowPlan();
            workflowPlan.setSequencerRun(sequencerRun);
            workflowPlan.setWorkflowRun(workflowRun);
            pipelineBeanService.getMaPSeqDAOBean().getWorkflowPlanDAO().save(workflowPlan);
        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }
    }

    private Set<String> findStudyName(String sampleSheetContent) throws IOException {
        logger.info("ENTERING findStudyName(File)");
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

    public PipelineBeanService getPipelineBeanService() {
        return pipelineBeanService;
    }

    public void setPipelineBeanService(PipelineBeanService pipelineBeanService) {
        this.pipelineBeanService = pipelineBeanService;
    }

}
