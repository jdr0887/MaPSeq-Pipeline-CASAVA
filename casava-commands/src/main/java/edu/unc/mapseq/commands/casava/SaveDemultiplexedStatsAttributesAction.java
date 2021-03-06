package edu.unc.mapseq.commands.casava;

import java.util.List;
import java.util.concurrent.Executors;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.AbstractAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.casava.SaveDemultiplexedStatsAttributesRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBean;

@Command(scope = "casava", name = "save-demultiplexed-stats-attributes", description = "Save Demultiplexed Stats Attributes")
public class SaveDemultiplexedStatsAttributesAction extends AbstractAction {

    private final Logger logger = LoggerFactory.getLogger(SaveDemultiplexedStatsAttributesAction.class);

    private MaPSeqDAOBean maPSeqDAOBean;

    @Argument(index = 0, name = "flowcellId", description = "Flowcell Identifier", required = true, multiValued = true)
    private List<Long> flowcellIdList;

    @Override
    protected Object doExecute() throws Exception {
        logger.debug("ENTERING doExecute()");
        SaveDemultiplexedStatsAttributesRunnable runnable = new SaveDemultiplexedStatsAttributesRunnable();
        runnable.setMapseqDAOBean(maPSeqDAOBean);
        runnable.setFlowcellIdList(flowcellIdList);
        Executors.newSingleThreadExecutor().execute(runnable);
        return null;
    }

    public MaPSeqDAOBean getMaPSeqDAOBean() {
        return maPSeqDAOBean;
    }

    public void setMaPSeqDAOBean(MaPSeqDAOBean maPSeqDAOBean) {
        this.maPSeqDAOBean = maPSeqDAOBean;
    }

    public List<Long> getFlowcellIdList() {
        return flowcellIdList;
    }

    public void setFlowcellIdList(List<Long> flowcellIdList) {
        this.flowcellIdList = flowcellIdList;
    }

}
