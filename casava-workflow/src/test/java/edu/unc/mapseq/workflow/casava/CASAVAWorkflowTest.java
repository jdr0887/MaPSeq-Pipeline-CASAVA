package edu.unc.mapseq.workflow.casava;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.jgrapht.DirectedGraph;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.renci.jlrm.condor.ext.CondorDOTExporter;

import edu.unc.mapseq.module.casava.ConfigureBCLToFastqCLI;
import edu.unc.mapseq.module.core.CopyCLI;
import edu.unc.mapseq.module.core.MakeCLI;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.impl.WorkflowJobFactory;

public class CASAVAWorkflowTest {

    @Test
    public void createDOT() {
        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        int count = 0;

        try {
            CondorJob configureBCLToFastQJob = WorkflowJobFactory
                    .createJob(++count, ConfigureBCLToFastqCLI.class, null).build();
            graph.addVertex(configureBCLToFastQJob);

            CondorJob makeJob = WorkflowJobFactory.createJob(++count, MakeCLI.class, null).build();
            graph.addVertex(makeJob);
            graph.addEdge(configureBCLToFastQJob, makeJob);

            CondorJob copyRead1Job = WorkflowJobFactory.createJob(++count, CopyCLI.class, null).build();
            graph.addVertex(copyRead1Job);
            graph.addEdge(makeJob, copyRead1Job);

            CondorJob copyRead2Job = WorkflowJobFactory.createJob(++count, CopyCLI.class, null).build();
            graph.addVertex(copyRead2Job);
            graph.addEdge(makeJob, copyRead2Job);
        } catch (WorkflowException e1) {
            e1.printStackTrace();
        }

        VertexNameProvider<CondorJob> vnpId = new VertexNameProvider<CondorJob>() {
            @Override
            public String getVertexName(CondorJob job) {
                return job.getName();
            }
        };

        VertexNameProvider<CondorJob> vnpLabel = new VertexNameProvider<CondorJob>() {
            @Override
            public String getVertexName(CondorJob job) {
                return job.getName();
            }
        };

        CondorDOTExporter<CondorJob, CondorJobEdge> dotExporter = new CondorDOTExporter<CondorJob, CondorJobEdge>(
                vnpId, vnpLabel, null, null, null, null);
        File srcSiteResourcesImagesDir = new File("../src/site/resources/images");
        if (!srcSiteResourcesImagesDir.exists()) {
            srcSiteResourcesImagesDir.mkdirs();
        }
        File dotFile = new File(srcSiteResourcesImagesDir, "workflow.dag.dot");
        try {
            FileWriter fw = new FileWriter(dotFile);
            dotExporter.export(fw, graph);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
