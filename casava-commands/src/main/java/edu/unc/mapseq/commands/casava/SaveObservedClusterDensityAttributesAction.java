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

@Command(scope = "casava", name = "save-observed-cluster-density-attributes", description = "Save Observed Cluster Density Attributes")
public class SaveObservedClusterDensityAttributesAction extends AbstractAction {

    private final Logger logger = LoggerFactory.getLogger(SaveObservedClusterDensityAttributesAction.class);

    private MaPSeqDAOBean maPSeqDAOBean;

    private MaPSeqConfigurationService maPSeqConfigurationService;

    @Argument(index = 0, name = "sequencerRunId", required = true, multiValued = true)
    private List<Long> sequencerRunIdList;

    @Override
    protected Object doExecute() throws Exception {
        logger.info("ENTERING doExecute()");
        SaveObservedClusterDensityAttributesRunnable runnable = new SaveObservedClusterDensityAttributesRunnable();
        runnable.setMapseqDAOBean(maPSeqDAOBean);
        runnable.setMapseqConfigurationService(maPSeqConfigurationService);
        runnable.setSequencerRunIdList(sequencerRunIdList);
        Executors.newSingleThreadExecutor().execute(runnable);
        return null;
    }

    public MaPSeqDAOBean getMaPSeqDAOBean() {
        return maPSeqDAOBean;
    }

    public void setMaPSeqDAOBean(MaPSeqDAOBean maPSeqDAOBean) {
        this.maPSeqDAOBean = maPSeqDAOBean;
    }

    public MaPSeqConfigurationService getMaPSeqConfigurationService() {
        return maPSeqConfigurationService;
    }

    public void setMaPSeqConfigurationService(MaPSeqConfigurationService maPSeqConfigurationService) {
        this.maPSeqConfigurationService = maPSeqConfigurationService;
    }

    public List<Long> getSequencerRunIdList() {
        return sequencerRunIdList;
    }

    public void setSequencerRunIdList(List<Long> sequencerRunIdList) {
        this.sequencerRunIdList = sequencerRunIdList;
    }

}
