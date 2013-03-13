package edu.unc.mapseq.pipeline.casava;

import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CasavaPipelineExecutorService {

    private final Logger logger = LoggerFactory.getLogger(CasavaPipelineExecutorService.class);

    private final Timer mainTimer = new Timer();

    private CasavaPipelineBeanService pipelineBeanService;

    public void start() throws Exception {
        logger.info("ENTERING stop()");

        long delay = 15 * 1000; // 15 seconds
        long period = 5 * 60 * 1000; // 5 minutes

        CasavaPipelineExecutorTask task = new CasavaPipelineExecutorTask();
        task.setPipelineBeanService(pipelineBeanService);
        mainTimer.scheduleAtFixedRate(task, delay, period);

    }

    public void stop() throws Exception {
        logger.info("ENTERING stop()");
        mainTimer.purge();
        mainTimer.cancel();
    }

    public CasavaPipelineBeanService getPipelineBeanService() {
        return pipelineBeanService;
    }

    public void setPipelineBeanService(CasavaPipelineBeanService pipelineBeanService) {
        this.pipelineBeanService = pipelineBeanService;
    }

}
