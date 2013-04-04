package edu.unc.mapseq.commands.casava;

import java.util.List;
import java.util.concurrent.Executors;

import org.apache.felix.gogo.commands.Argument;
import org.apache.felix.gogo.commands.Command;
import org.apache.karaf.shell.console.AbstractAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.casava.SaveObservedClusterDensityAttributesRunnable;
import edu.unc.mapseq.config.MaPSeqConfigurationService;
import edu.unc.mapseq.dao.MaPSeqDAOBean;

@Command(scope = "mapseq", name = "save-observed-cluster-density-attributes", description = "Save Observed Cluster Density Attributes")
public class SaveObservedClusterDensityAttributesAction extends AbstractAction {

    private final Logger logger = LoggerFactory.getLogger(SaveObservedClusterDensityAttributesAction.class);

    private MaPSeqDAOBean mapseqDAOBean;

    private MaPSeqConfigurationService mapseqConfigurationService;

    @Argument(index = 0, name = "sequencerRunId", required = true, multiValued = true)
    private List<Long> sequencerRunIdList;

    @Override
    protected Object doExecute() throws Exception {
        logger.info("ENTERING doExecute()");
        SaveObservedClusterDensityAttributesRunnable runnable = new SaveObservedClusterDensityAttributesRunnable();
        runnable.setMapseqDAOBean(mapseqDAOBean);
        runnable.setMapseqConfigurationService(mapseqConfigurationService);
        runnable.setSequencerRunIdList(sequencerRunIdList);
        Executors.newSingleThreadExecutor().execute(runnable);
        return null;
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

    public List<Long> getSequencerRunIdList() {
        return sequencerRunIdList;
    }

    public void setSequencerRunIdList(List<Long> sequencerRunIdList) {
        this.sequencerRunIdList = sequencerRunIdList;
    }

}
