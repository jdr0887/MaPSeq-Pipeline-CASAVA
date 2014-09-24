package edu.unc.mapseq.commons.casava;

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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import edu.unc.mapseq.dao.FlowcellDAO;
import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Sample;

public class SaveDemultiplexedStatsAttributesRunnable implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(SaveDemultiplexedStatsAttributesRunnable.class);

    private MaPSeqDAOBean mapseqDAOBean;

    private List<Long> flowcellIdList;

    public SaveDemultiplexedStatsAttributesRunnable() {
        super();
    }

    @Override
    public void run() {
        logger.debug("ENTERING run()");

        List<Flowcell> fList = new ArrayList<Flowcell>();

        try {

            FlowcellDAO flowcellDAO = mapseqDAOBean.getFlowcellDAO();
            for (Long flowcellId : getFlowcellIdList()) {
                Flowcell flowcell = flowcellDAO.findById(flowcellId);
                if (flowcell != null) {
                    fList.add(flowcell);
                }
            }

            if (!fList.isEmpty()) {

                for (Flowcell flowcell : fList) {

                    File flowcellDir = new File(flowcell.getBaseDirectory(), flowcell.getName());
                    String flowcellProper = null;

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
                        Node runFlowcellIdNode = (Node) xpath
                                .evaluate(runFlowcellIdPath, document, XPathConstants.NODE);
                        flowcellProper = runFlowcellIdNode.getTextContent();
                        logger.debug("flowcell = {}", flowcellProper);

                    } catch (XPathExpressionException | DOMException | ParserConfigurationException | SAXException
                            | IOException e) {
                        e.printStackTrace();
                    }

                    List<Sample> sampleList = mapseqDAOBean.getSampleDAO().findByFlowcellId(flowcell.getId());

                    if (sampleList == null) {
                        logger.warn("sampleList was null");
                        return;
                    }

                    for (Sample sample : sampleList) {

                        File unalignedDir = new File(flowcellDir, String.format("Unaligned.%d", sample.getLaneIndex()));
                        File baseCallStatsDir = new File(unalignedDir, String.format("Basecall_Stats_%s",
                                flowcellProper));
                        File statsFile = new File(baseCallStatsDir, "Demultiplex_Stats.htm");

                        if (!statsFile.exists()) {
                            logger.warn("statsFile doesn't exist: {}", statsFile.getAbsolutePath());
                            continue;
                        }

                        logger.info("parsing statsFile: {}", statsFile.getAbsolutePath());

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

                                    Set<Attribute> attributeSet = sample.getAttributes();

                                    if (attributeSet == null) {
                                        attributeSet = new HashSet<Attribute>();
                                    }

                                    Set<String> entityAttributeNameSet = new HashSet<String>();

                                    for (Attribute attribute : attributeSet) {
                                        entityAttributeNameSet.add(attribute.getName());
                                    }

                                    Set<String> synchSet = Collections.synchronizedSet(entityAttributeNameSet);

                                    if (StringUtils.isNotEmpty(yeildElement.text())) {
                                        String value = yeildElement.text().replace(",", "");
                                        if (synchSet.contains("yield")) {
                                            for (Attribute attribute : attributeSet) {
                                                if (attribute.getName().equals("yield")) {
                                                    attribute.setValue(value);
                                                    break;
                                                }
                                            }
                                        } else {
                                            attributeSet.add(new Attribute("yield", value));
                                        }
                                    }

                                    if (StringUtils.isNotEmpty(passingFilteringElement.text())) {
                                        String value = passingFilteringElement.text();
                                        if (synchSet.contains("passedFiltering")) {
                                            for (Attribute attribute : attributeSet) {
                                                if (attribute.getName().equals("passedFiltering")) {
                                                    attribute.setValue(value);
                                                    break;
                                                }
                                            }
                                        } else {
                                            attributeSet.add(new Attribute("passedFiltering", value));
                                        }
                                    }

                                    if (StringUtils.isNotEmpty(numberOfReadsElement.text())) {
                                        String value = numberOfReadsElement.text().replace(",", "");
                                        if (synchSet.contains("numberOfReads")) {
                                            for (Attribute attribute : attributeSet) {
                                                if (attribute.getName().equals("numberOfReads")) {
                                                    attribute.setValue(value);
                                                    break;
                                                }
                                            }
                                        } else {
                                            attributeSet.add(new Attribute("numberOfReads", value));
                                        }
                                    }

                                    if (StringUtils.isNotEmpty(rawClustersPerLaneElement.text())) {
                                        String value = rawClustersPerLaneElement.text();
                                        if (synchSet.contains("rawClustersPerLane")) {
                                            for (Attribute attribute : attributeSet) {
                                                if (attribute.getName().equals("rawClustersPerLane")) {
                                                    attribute.setValue(value);
                                                    break;
                                                }
                                            }
                                        } else {
                                            attributeSet.add(new Attribute("rawClustersPerLane", value));
                                        }
                                    }

                                    if (StringUtils.isNotEmpty(perfectIndexReadsElement.text())) {
                                        String value = perfectIndexReadsElement.text();
                                        if (synchSet.contains("perfectIndexReads")) {
                                            for (Attribute attribute : attributeSet) {
                                                if (attribute.getName().equals("perfectIndexReads")) {
                                                    attribute.setValue(value);
                                                    break;
                                                }
                                            }
                                        } else {
                                            attributeSet.add(new Attribute("perfectIndexReads", value));
                                        }
                                    }

                                    if (StringUtils.isNotEmpty(oneMismatchReadsIndexElement.text())) {
                                        String value = oneMismatchReadsIndexElement.text();
                                        if (synchSet.contains("oneMismatchReadsIndex")) {
                                            for (Attribute attribute : attributeSet) {
                                                if (attribute.getName().equals("oneMismatchReadsIndex")) {
                                                    attribute.setValue(value);
                                                    break;
                                                }
                                            }
                                        } else {
                                            attributeSet.add(new Attribute("oneMismatchReadsIndex", value));
                                        }
                                    }

                                    if (StringUtils.isNotEmpty(q30YeildPassingFilteringElement.text())) {
                                        String value = q30YeildPassingFilteringElement.text();
                                        if (synchSet.contains("q30YieldPassingFiltering")) {
                                            for (Attribute attribute : attributeSet) {
                                                if (attribute.getName().equals("q30YieldPassingFiltering")) {
                                                    attribute.setValue(value);
                                                    break;
                                                }
                                            }
                                        } else {
                                            attributeSet.add(new Attribute("q30YieldPassingFiltering", value));
                                        }
                                    }

                                    if (StringUtils.isNotEmpty(meanQualityScorePassingFilteringElement.text())) {
                                        String value = meanQualityScorePassingFilteringElement.text();
                                        if (synchSet.contains("meanQualityScorePassingFiltering")) {
                                            for (Attribute attribute : attributeSet) {
                                                if (attribute.getName().equals("meanQualityScorePassingFiltering")) {
                                                    attribute.setValue(value);
                                                    break;
                                                }
                                            }
                                        } else {
                                            attributeSet.add(new Attribute("meanQualityScorePassingFiltering", value));
                                        }
                                    }

                                    sample.setAttributes(attributeSet);
                                    mapseqDAOBean.getSampleDAO().save(sample);
                                    System.out.println(String.format("Successfully saved sample: %s", sample.getId()));
                                    logger.info(sample.toString());
                                }

                            }
                        } catch (NumberFormatException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
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
