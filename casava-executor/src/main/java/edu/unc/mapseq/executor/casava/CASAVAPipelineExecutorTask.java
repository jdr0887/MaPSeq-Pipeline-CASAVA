package edu.unc.mapseq.executor.casava;

import java.util.Date;
import java.util.List;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.WorkflowPlanDAO;
import edu.unc.mapseq.dao.WorkflowRunDAO;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowPlan;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.pipeline.PipelineBeanService;
import edu.unc.mapseq.pipeline.PipelineExecutor;
import edu.unc.mapseq.pipeline.PipelineTPE;
import edu.unc.mapseq.pipeline.casava.CASAVAPipeline;

public class CASAVAPipelineExecutorTask extends TimerTask {

    private final Logger logger = LoggerFactory.getLogger(CASAVAPipelineExecutorTask.class);

    private final PipelineTPE threadPoolExecutor = new PipelineTPE();

    private PipelineBeanService pipelineBeanService;

    public CASAVAPipelineExecutorTask() {
        super();
    }

    @Override
    public void run() {
        logger.info("ENTERING run()");

        threadPoolExecutor.setCorePoolSize(pipelineBeanService.getCorePoolSize());
        threadPoolExecutor.setMaximumPoolSize(pipelineBeanService.getMaxPoolSize());

        logger.info(String.format("ActiveCount: %d, TaskCount: %d, CompletedTaskCount: %d",
                threadPoolExecutor.getActiveCount(), threadPoolExecutor.getTaskCount(),
                threadPoolExecutor.getCompletedTaskCount()));

        WorkflowDAO workflowDAO = this.pipelineBeanService.getMaPSeqDAOBean().getWorkflowDAO();
        WorkflowRunDAO workflowRunDAO = this.pipelineBeanService.getMaPSeqDAOBean().getWorkflowRunDAO();
        WorkflowPlanDAO workflowPlanDAO = this.pipelineBeanService.getMaPSeqDAOBean().getWorkflowPlanDAO();

        try {
            Workflow workflow = workflowDAO.findByName("CASAVA");
            List<WorkflowPlan> workflowPlanList = workflowPlanDAO.findEnqueued(workflow.getId());

            if (workflowPlanList != null && workflowPlanList.size() > 0) {

                logger.info("dequeuing {} WorkflowPlans", workflowPlanList.size());
                for (WorkflowPlan workflowPlan : workflowPlanList) {

                    CASAVAPipeline pipeline = new CASAVAPipeline();

                    WorkflowRun workflowRun = workflowPlan.getWorkflowRun();
                    workflowRun.setVersion(pipeline.getVersion());
                    workflowRun.setDequeuedDate(new Date());
                    workflowRunDAO.save(workflowRun);

                    pipeline.setPipelineBeanService(pipelineBeanService);
                    pipeline.setWorkflowPlan(workflowPlan);
                    threadPoolExecutor.submit(new PipelineExecutor(pipeline));

                }

            }

        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

    }

    public PipelineBeanService getPipelineBeanService() {
        return pipelineBeanService;
    }

    public void setPipelineBeanService(PipelineBeanService pipelineBeanService) {
        this.pipelineBeanService = pipelineBeanService;
    }

}
