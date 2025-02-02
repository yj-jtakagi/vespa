// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.maintenance;

import com.yahoo.config.provision.ClusterSpec;
import com.yahoo.vespa.hosted.controller.Application;
import com.yahoo.vespa.hosted.controller.Controller;
import com.yahoo.vespa.hosted.controller.api.identifiers.DeploymentId;
import com.yahoo.vespa.hosted.controller.api.integration.configserver.NodeRepository;
import com.yahoo.vespa.hosted.controller.api.integration.noderepository.NodeList;
import com.yahoo.vespa.hosted.controller.api.integration.noderepository.NodeRepositoryNode;
import com.yahoo.vespa.hosted.controller.application.ClusterInfo;
import com.yahoo.vespa.hosted.controller.application.Deployment;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Maintains information about hardware, hostnames and cluster specifications.
 *
 * This is used to calculate cost metrics for the application api.
 *
 * @author smorgrav
 */
public class ClusterInfoMaintainer extends Maintainer {

    private static final Logger log = Logger.getLogger(ClusterInfoMaintainer.class.getName());
    
    private final Controller controller;
    private final NodeRepository nodeRepository;

    ClusterInfoMaintainer(Controller controller, Duration duration, JobControl jobControl,
                          NodeRepository nodeRepository) {
        super(controller, duration, jobControl);
        this.controller = controller;
        this.nodeRepository = nodeRepository;
    }

    private static String clusterId(NodeRepositoryNode node) {
        return node.getMembership().clusterid;
    }

    private Map<ClusterSpec.Id, ClusterInfo> getClusterInfo(NodeList nodes) {
        Map<ClusterSpec.Id, ClusterInfo> infoMap = new HashMap<>();

        // Group nodes by clusterid
        Map<String, List<NodeRepositoryNode>> clusters = nodes.nodes().stream()
                .filter(node -> node.getMembership() != null)
                .collect(Collectors.groupingBy(ClusterInfoMaintainer::clusterId));

        // For each cluster - get info
        for (String id : clusters.keySet()) {
            List<NodeRepositoryNode> clusterNodes = clusters.get(id);

            // Assume they are all equal and use first node as a representative for the cluster
            NodeRepositoryNode node = clusterNodes.get(0);

            // Extract flavor info
            double cpu = 0;
            double mem = 0;
            double disk = 0;

            // Add to map
            List<String> hostnames = clusterNodes.stream().map(NodeRepositoryNode::getHostname).collect(Collectors.toList());
            int cost = node.getCost() == null ? 0 : node.getCost(); // Cost is not guaranteed to be defined for all flavors
            ClusterInfo inf = new ClusterInfo(node.getFlavor(), cost, cpu, mem, disk,
                                              ClusterSpec.Type.from(node.getMembership().clustertype), hostnames);
            infoMap.put(new ClusterSpec.Id(id), inf);
        }

        return infoMap;
    }

    @Override
    protected void maintain() {
        for (Application application : controller().applications().asList()) {
            for (Deployment deployment : application.deployments().values()) {
                DeploymentId deploymentId = new DeploymentId(application.id(), deployment.zone());
                try {
                    NodeList nodes = nodeRepository.listNodes(deploymentId.zoneId(), deploymentId.applicationId());
                    Map<ClusterSpec.Id, ClusterInfo> clusterInfo = getClusterInfo(nodes);
                    controller().applications().lockIfPresent(application.id(), lockedApplication ->
                        controller.applications().store(lockedApplication.withClusterInfo(deployment.zone(), clusterInfo)));
                } catch (Exception e) {
                    log.log(Level.WARNING, "Failing getting cluster information for " + deploymentId, e);
                }
            }
        }
    }

}
