package edu.unc.mapseq.commands.casava;

import java.util.List;
import java.util.concurrent.Executors;

import org.apache.felix.gogo.commands.Argument;
import org.apache.felix.gogo.commands.Command;
import org.apache.karaf.shell.console.AbstractAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.casava.FixMismappedFastqFileDataRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBean;

@Command(scope = "casava", name = "fix-mismapped-fastq-file-data", description = "Fix Mismapped Fastq FileData")
public class FixMisappedFastqFileDataAction extends AbstractAction {

    private final Logger logger = LoggerFactory.getLogger(FixMisappedFastqFileDataAction.class);

    private MaPSeqDAOBean maPSeqDAOBean;

    @Argument(index = 0, name = "sequencerRunId", description = "Sequencer Run Identifier", required = true, multiValued = true)
    private List<Long> sequencerRunIdList;

    @Override
    protected Object doExecute() throws Exception {
        logger.debug("ENTERING doExecute()");
        FixMismappedFastqFileDataRunnable fixMisMappedSamplesRunnable = new FixMismappedFastqFileDataRunnable();
        fixMisMappedSamplesRunnable.setMapseqDAOBean(maPSeqDAOBean);
        fixMisMappedSamplesRunnable.setSequencerRunIdList(sequencerRunIdList);
        Executors.newSingleThreadExecutor().execute(fixMisMappedSamplesRunnable);

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
