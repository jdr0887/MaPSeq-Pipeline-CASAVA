package edu.unc.mapseq.commands.casava;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
import org.apache.felix.gogo.commands.Argument;
import org.apache.felix.gogo.commands.Command;
import org.apache.karaf.shell.console.AbstractAction;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SequencerRunDAO;
import edu.unc.mapseq.dao.model.EntityAttribute;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.SequencerRun;

@Command(scope = "mapseq", name = "save-demultiplexed-stats-attributes", description = "Save Demultiplexed Stats Attributes")
public class SaveDemultiplexedStatsAttributesAction extends AbstractAction {

    private final Logger logger = LoggerFactory.getLogger(SaveDemultiplexedStatsAttributesAction.class);

    private MaPSeqDAOBean mapseqDAOBean;

    @Argument(index = 0, name = "sequencerRunId", description = "Sequencer Run Identifier", required = true, multiValued = true)
    private List<Long> sequencerRunIdList;

    @Override
    protected Object doExecute() throws Exception {

        List<SequencerRun> srList = new ArrayList<SequencerRun>();
        SequencerRunDAO sequencerRunDAO = mapseqDAOBean.getSequencerRunDAO();
        for (Long sequencerRunId : sequencerRunIdList) {
            SequencerRun sequencerRun = null;
            try {
                sequencerRun = sequencerRunDAO.findById(sequencerRunId);
            } catch (MaPSeqDAOException e) {
            }
            if (sequencerRun != null) {
                srList.add(sequencerRun);
            }
        }

        if (srList.size() > 0) {

            for (SequencerRun sr : srList) {

                File sequencerRunDir = new File(sr.getBaseDirectory(), sr.getName());
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

                } catch (XPathExpressionException | DOMException | ParserConfigurationException | SAXException
                        | IOException e) {
                    e.printStackTrace();
                }

                List<HTSFSample> htsfSampleList = null;
                try {
                    htsfSampleList = mapseqDAOBean.getHTSFSampleDAO().findBySequencerRunId(sr.getId());
                } catch (MaPSeqDAOException e) {
                }

                if (htsfSampleList == null) {
                    logger.warn("htsfSampleList was null");
                    return null;
                }

                for (HTSFSample sample : htsfSampleList) {

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
                                            attributeSet.add(new EntityAttribute("meanQualityScorePassingFiltering",
                                                    value));
                                        }
                                    }

                                    sample.setAttributes(attributeSet);
                                    mapseqDAOBean.getHTSFSampleDAO().save(sample);
                                    System.out.println(String.format("Successfully saved sample: %s", sample.getId()));
                                    logger.info(sample.toString());
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

            }

        }
        return null;
    }

    public MaPSeqDAOBean getMapseqDAOBean() {
        return mapseqDAOBean;
    }

    public void setMapseqDAOBean(MaPSeqDAOBean mapseqDAOBean) {
        this.mapseqDAOBean = mapseqDAOBean;
    }

    public List<Long> getSequencerRunIdList() {
        return sequencerRunIdList;
    }

    public void setSequencerRunIdList(List<Long> sequencerRunIdList) {
        this.sequencerRunIdList = sequencerRunIdList;
    }

}
