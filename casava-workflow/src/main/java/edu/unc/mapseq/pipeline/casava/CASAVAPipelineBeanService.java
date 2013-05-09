package edu.unc.mapseq.pipeline.casava;

import edu.unc.mapseq.pipeline.AbstractPipelineBeanService;

public class CASAVAPipelineBeanService extends AbstractPipelineBeanService {

    private String siteName;

    public CASAVAPipelineBeanService() {
        super();
    }

    public String getSiteName() {
        return siteName;
    }

    public void setSiteName(String siteName) {
        this.siteName = siteName;
    }

}
