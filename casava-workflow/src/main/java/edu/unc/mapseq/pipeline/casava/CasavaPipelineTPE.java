package edu.unc.mapseq.pipeline.casava;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CasavaPipelineTPE extends ThreadPoolExecutor {

    public CasavaPipelineTPE() {
        super(20, 20, 5L, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
    }

}
