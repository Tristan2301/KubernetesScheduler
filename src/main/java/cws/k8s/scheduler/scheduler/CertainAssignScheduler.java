package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.csv_reader.ReadCsv;
import cws.k8s.scheduler.model.*;
import cws.k8s.scheduler.scheduler.prioritize.Prioritize;
import cws.k8s.scheduler.client.Informable;
import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.scheduler.nodeassign.NodeAssign;
import cws.k8s.scheduler.util.NodeTaskAlignment;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

@Slf4j
public class CertainAssignScheduler extends Scheduler {

    private final Prioritize prioritize;
    private final NodeAssign nodeAssigner;
    private final ReadCsv readCsv;

    public CertainAssignScheduler( String execution,
                                      KubernetesClient client,
                                      String namespace,
                                      SchedulerConfig config,
                                      Prioritize prioritize,
                                      NodeAssign nodeAssigner,
                                      ReadCsv readCsv,
                                      String pathToCsv) {
        super(execution, client, namespace, config);
        this.prioritize = prioritize;
        this.nodeAssigner = nodeAssigner;
        this.readCsv = new ReadCsv(pathToCsv);
        nodeAssigner.registerScheduler( this );
        if ( nodeAssigner instanceof Informable ){
            client.addInformable( (Informable) nodeAssigner );
        }
    }

    @Override
    public void close() {
        super.close();
        if ( nodeAssigner instanceof Informable ){
            client.removeInformable( (Informable) nodeAssigner );
        }
    } /// was macht das

    @Override
    public ScheduleObject getTaskNodeAlignment(
            final List<Task> unscheduledTasks,
            final Map<NodeWithAlloc, Requirements> availableByNode
    ){
        final ArrayList<Map.Entry<NodeWithAlloc, Requirements>> entries = new ArrayList<>( availableByNode.entrySet() );
        List<NodeTaskAlignment> alignment = new LinkedList<>();

        readCsv.readAndProcessCsv(null);
        final Map<String, Pair<String, Integer>> labeltonoderesouce = readCsv.getLabelNameToNodeResource();

        for ( Task unscheduledTask : unscheduledTasks ) {
            final String taskLabel = unscheduledTask.getProcess().getLabel();
            if(labeltonoderesouce.containsKey(taskLabel)){
                Pair<String, Integer> nodeResourcePair = labeltonoderesouce.get(taskLabel);
                String nodeName = nodeResourcePair.getLeft();
                // int resource = nodeResourcePair.getRight();  // add resource cap 

                for ( Map.Entry<NodeWithAlloc, Requirements> e : entries ) {
                    final NodeWithAlloc node = e.getKey();
                   
                    if(nodeName == node.getName()){
                        alignment.add( new NodeTaskAlignment( node, unscheduledTask ) );
                    }
                }
            }
        }


        // long start = System.currentTimeMillis();
        // if ( traceEnabled ) {
        //     int index = 1;
        //     for ( Task unscheduledTask : unscheduledTasks ) {
        //         unscheduledTask.getTraceRecord().setSchedulerPlaceInQueue( index++ );
        //     }
        // }
        // prioritize.sortTasks( unscheduledTasks );
        // List<NodeTaskAlignment> alignment = nodeAssigner.getTaskNodeAlignment(unscheduledTasks, availableByNode);
        // long timeDelta = System.currentTimeMillis() - start;
        // for ( Task unscheduledTask : unscheduledTasks ) {
        //     unscheduledTask.getTraceRecord().setSchedulerTimeToSchedule( (int) timeDelta );
        // }

        final ScheduleObject scheduleObject = new ScheduleObject(alignment);
        scheduleObject.setCheckStillPossible( false );
        return scheduleObject;
    }

}
