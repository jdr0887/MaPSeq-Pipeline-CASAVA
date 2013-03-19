package edu.unc.mapseq.pipeline.casava;

import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CASAVAPipelineExecutorService {

    private final Logger logger = LoggerFactory.getLogger(CASAVAPipelineExecutorService.class);

    private final Timer mainTimer = new Timer();

    private CASAVAPipelineBeanService pipelineBeanService;

    public void start() throws Exception {
        logger.info("ENTERING stop()");

        long delay = 1 * 60 * 1000; // 1 minute
        long period = 5 * 60 * 1000; // 5 minutes

        CASAVAPipelineExecutorTask task = new CASAVAPipelineExecutorTask();
        task.setPipelineBeanService(pipelineBeanService);
        mainTimer.scheduleAtFixedRate(task, delay, period);

    }

    public void stop() throws Exception {
        logger.info("ENTERING stop()");
        mainTimer.purge();
        mainTimer.cancel();
    }

    public CASAVAPipelineBeanService getPipelineBeanService() {
        return pipelineBeanService;
    }

    public void setPipelineBeanService(CASAVAPipelineBeanService pipelineBeanService) {
        this.pipelineBeanService = pipelineBeanService;
    }

}
