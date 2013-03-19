package edu.unc.mapseq.pipeline.casava;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CASAVAPipelineTPE extends ThreadPoolExecutor {

    public CASAVAPipelineTPE() {
        super(20, 20, 5L, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
    }

}
