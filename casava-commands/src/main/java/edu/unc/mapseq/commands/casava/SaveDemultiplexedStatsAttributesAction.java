package edu.unc.mapseq.commands.casava;

import java.util.List;
import java.util.concurrent.Executors;

import org.apache.felix.gogo.commands.Argument;
import org.apache.felix.gogo.commands.Command;
import org.apache.karaf.shell.console.AbstractAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.casava.SaveDemultiplexedStatsAttributesRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBean;

@Command(scope = "casava", name = "save-demultiplexed-stats-attributes", description = "Save Demultiplexed Stats Attributes")
public class SaveDemultiplexedStatsAttributesAction extends AbstractAction {

    private final Logger logger = LoggerFactory.getLogger(SaveDemultiplexedStatsAttributesAction.class);

    private MaPSeqDAOBean maPSeqDAOBean;

    @Argument(index = 0, name = "sequencerRunId", description = "Sequencer Run Identifier", required = true, multiValued = true)
    private List<Long> sequencerRunIdList;

    @Override
    protected Object doExecute() throws Exception {
        logger.debug("ENTERING doExecute()");
        SaveDemultiplexedStatsAttributesRunnable runnable = new SaveDemultiplexedStatsAttributesRunnable();
        runnable.setMapseqDAOBean(maPSeqDAOBean);
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

    public List<Long> getSequencerRunIdList() {
        return sequencerRunIdList;
    }

    public void setSequencerRunIdList(List<Long> sequencerRunIdList) {
        this.sequencerRunIdList = sequencerRunIdList;
    }

}
