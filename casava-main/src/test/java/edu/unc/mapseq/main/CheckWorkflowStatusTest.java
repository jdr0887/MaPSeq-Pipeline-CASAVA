package edu.unc.mapseq.main;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;

import org.junit.Test;

import edu.unc.mapseq.dao.HTSFSampleDAO;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.WorkflowPlanDAO;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.WorkflowPlan;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.ws.WebServiceDAOManager;

public class CheckWorkflowStatusTest {

    @Test
    public void testWorkflowPlan() {

        WebServiceDAOManager daoMgr = WebServiceDAOManager.getInstance();

        List<WorkflowPlan> wpList = new ArrayList<WorkflowPlan>();
        WorkflowPlanDAO workflowPlanDAO = daoMgr.getWSDAOBean().getWorkflowPlanDAO();
        try {
            List<WorkflowPlan> wfPlanList = workflowPlanDAO.findByStudyName("NC_GENES");
            if (wfPlanList != null) {
                wpList.addAll(wfPlanList);
            }
        } catch (MaPSeqDAOException e) {
        }
        HTSFSampleDAO hTSFSampleDAO = daoMgr.getWSDAOBean().getHTSFSampleDAO();

        try {

            if (wpList.size() > 0) {
                StringBuilder sb = new StringBuilder();
                Formatter formatter = new Formatter(sb, Locale.US);
                formatter
                        .format("%1$-18s %2$-18s %3$-30s %4$-18s %5$-20s %6$-24s %7$s%n", "SequencerRun ID",
                                "HTSFSample ID", "Sample Name", "Workflow Name", "Start Date", "End Date",
                                "WorkflowRun Status");

                Collections.sort(wpList, new Comparator<WorkflowPlan>() {
                    @Override
                    public int compare(WorkflowPlan wp1, WorkflowPlan wp2) {
                        WorkflowRun wr1 = wp1.getWorkflowRun();
                        WorkflowRun wr2 = wp2.getWorkflowRun();
                        if (wr1.getStartDate() != null && wr2.getStartDate() != null) {
                            if (wr1.getStartDate().before(wr2.getStartDate())) {
                                return -1;
                            }
                            if (wr1.getStartDate().after(wr2.getStartDate())) {
                                return 1;
                            }
                            if (wr1.getStartDate().equals(wr2.getStartDate())) {
                                return 0;
                            }
                        }
                        if (wr1.getStartDate() != null && wr2.getStartDate() == null) {
                            return -1;
                        }
                        if (wr1.getStartDate() == null && wr2.getStartDate() == null) {
                            return 1;
                        }
                        return wr1.getCreationDate().compareTo(wr2.getCreationDate());
                    }
                });

                for (WorkflowPlan wp : wpList) {

                    List<HTSFSample> htsfSampleList = hTSFSampleDAO.findBySequencerRunId(wp.getSequencerRun().getId());
                    WorkflowRun wr = wp.getWorkflowRun();
                    if (htsfSampleList != null && htsfSampleList.size() > 0) {
                        for (HTSFSample sample : htsfSampleList) {
                            Date startDate = wr.getStartDate();
                            String formattedStartDate = "";
                            if (startDate != null) {
                                formattedStartDate = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT)
                                        .format(wr.getStartDate());
                            }
                            Date endDate = wr.getEndDate();
                            String formattedEndDate = "";
                            if (endDate != null) {
                                formattedEndDate = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT)
                                        .format(wr.getEndDate());
                            }
                            formatter.format("%1$-18s %2$-18s %3$-30s %4$-18s %5$-20s %6$-24s %7$s%n", wp
                                    .getSequencerRun().getId(), sample.getId(), sample.getName(), wr.getWorkflow()
                                    .getName(), formattedStartDate, formattedEndDate, wr.getStatus().getState());
                        }
                    }
                    formatter.flush();
                }
                System.out.println(formatter.toString());
                formatter.close();
            }
        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

    }

}
