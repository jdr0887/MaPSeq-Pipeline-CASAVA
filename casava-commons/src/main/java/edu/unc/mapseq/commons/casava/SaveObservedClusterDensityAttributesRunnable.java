package edu.unc.mapseq.commons.casava;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.config.MaPSeqConfigurationService;
import edu.unc.mapseq.dao.FlowcellDAO;
import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Sample;

public class SaveObservedClusterDensityAttributesRunnable implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(SaveObservedClusterDensityAttributesRunnable.class);

    private MaPSeqDAOBean mapseqDAOBean;

    private MaPSeqConfigurationService mapseqConfigurationService;

    private List<Long> flowcellIdList;

    public SaveObservedClusterDensityAttributesRunnable() {
        super();
    }

    @Override
    public void run() {
        logger.debug("ENTERING run()");

        List<Flowcell> fList = new ArrayList<Flowcell>();

        FlowcellDAO flowcellDAO = mapseqDAOBean.getFlowcellDAO();
        for (Long flowcellId : getFlowcellIdList()) {
            try {
                Flowcell flowcell = flowcellDAO.findById(flowcellId);
                if (flowcell != null) {
                    fList.add(flowcell);
                }
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
            }
        }

        if (!fList.isEmpty()) {

            for (Flowcell flowcell : fList) {

                File flowcellDir = new File(flowcell.getBaseDirectory(), flowcell.getName());
                File dataDir = new File(flowcellDir, "Data");
                File reportsDir = new File(dataDir, "reports");
                File numClustersByLaneFile = new File(reportsDir, "NumClusters By Lane.txt");
                logger.info("numClustersByLaneFile = {}", numClustersByLaneFile.getAbsolutePath());
                if (!numClustersByLaneFile.exists()) {
                    logger.warn("numClustersByLaneFile does not exist");
                    continue;
                }

                try {

                    List<Sample> sampleList = mapseqDAOBean.getSampleDAO().findByFlowcellId(flowcell.getId());
                    Map<Integer, List<Double>> laneClusterDensityTotalMap = new HashMap<Integer, List<Double>>();

                    for (Sample sample : sampleList) {
                        if (!laneClusterDensityTotalMap.containsKey(sample.getLaneIndex())) {
                            laneClusterDensityTotalMap.put(sample.getLaneIndex(), new ArrayList<Double>());
                        }
                    }

                    BufferedReader br = new BufferedReader(new FileReader(numClustersByLaneFile));
                    // skip the first 11 lines
                    for (int i = 0; i < 11; ++i) {
                        br.readLine();
                    }
                    String line;
                    while ((line = br.readLine()) != null) {
                        Integer lane = Integer.valueOf(StringUtils.split(line)[0]);
                        Double clusterDensity = Double.valueOf(StringUtils.split(line)[2]);
                        if (laneClusterDensityTotalMap.containsKey(lane + 1)) {
                            laneClusterDensityTotalMap.get(lane + 1).add(clusterDensity);
                        }
                    }
                    br.close();

                    for (Sample sample : sampleList) {
                        List<Double> laneClusterDensityTotalList = laneClusterDensityTotalMap
                                .get(sample.getLaneIndex());
                        long clusterDensityTotal = 0;
                        for (Double clusterDensity : laneClusterDensityTotalList) {
                            clusterDensityTotal += clusterDensity;
                        }
                        String value = (double) (clusterDensityTotal / laneClusterDensityTotalList.size()) / 1000 + "";
                        logger.info("value = {}", value);

                        Set<Attribute> attributeSet = sample.getAttributes();

                        Set<String> entityAttributeNameSet = new HashSet<String>();

                        if (!attributeSet.isEmpty()) {
                            for (Attribute attribute : attributeSet) {
                                entityAttributeNameSet.add(attribute.getName());
                            }
                        }

                        Set<String> synchSet = Collections.synchronizedSet(entityAttributeNameSet);

                        if (StringUtils.isNotEmpty(value)) {
                            if (synchSet.contains("observedClusterDensity")) {
                                for (Attribute attribute : attributeSet) {
                                    if (attribute.getName().equals("observedClusterDensity")) {
                                        attribute.setValue(value);
                                        break;
                                    }
                                }
                            } else {
                                attributeSet.add(new Attribute("observedClusterDensity", value));
                            }
                        }
                        sample.setAttributes(attributeSet);
                        mapseqDAOBean.getSampleDAO().save(sample);
                    }

                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (MaPSeqDAOException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    public MaPSeqDAOBean getMapseqDAOBean() {
        return mapseqDAOBean;
    }

    public void setMapseqDAOBean(MaPSeqDAOBean mapseqDAOBean) {
        this.mapseqDAOBean = mapseqDAOBean;
    }

    public MaPSeqConfigurationService getMapseqConfigurationService() {
        return mapseqConfigurationService;
    }

    public void setMapseqConfigurationService(MaPSeqConfigurationService mapseqConfigurationService) {
        this.mapseqConfigurationService = mapseqConfigurationService;
    }

    public List<Long> getFlowcellIdList() {
        return flowcellIdList;
    }

    public void setFlowcellIdList(List<Long> flowcellIdList) {
        this.flowcellIdList = flowcellIdList;
    }

}
