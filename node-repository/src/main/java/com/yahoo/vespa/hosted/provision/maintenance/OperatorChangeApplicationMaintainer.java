// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.provision.maintenance;

import com.yahoo.config.provision.ApplicationId;
import com.yahoo.config.provision.Deployer;
import com.yahoo.config.provision.NodeType;
import com.yahoo.vespa.hosted.provision.Node;
import com.yahoo.vespa.hosted.provision.NodeRepository;
import com.yahoo.vespa.hosted.provision.node.Agent;
import com.yahoo.vespa.hosted.provision.node.History;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The application maintainer detects manual operator changes to nodes and redeploys affected applications.
 * The purpose of this is to redeploy affected applications faster than achieved by the regular application
 * maintenance to reduce the time period where the node repository and the application model is out of sync.
 * 
 * Why can't the manual change directly make the application redeployment?
 * Because the redeployment must run at the right config server, while the node state change may be running
 * at any config server.
 *
 * @author bratseth
 */
public class OperatorChangeApplicationMaintainer extends ApplicationMaintainer {
    
    OperatorChangeApplicationMaintainer(Deployer deployer, NodeRepository nodeRepository, Duration interval) {
        super(deployer, nodeRepository, interval);
    }

    @Override
    protected Set<ApplicationId> applicationsNeedingMaintenance() {
        Map<ApplicationId, List<Node>> nodesByApplication = nodeRepository().getNodes(NodeType.tenant).stream()
                .filter(node -> node.allocation().isPresent())
                .collect(Collectors.groupingBy(node -> node.allocation().get().owner(), Collectors.toList()));

        return nodesByApplication.entrySet().stream()
                .filter(entry -> entry.getValue().stream()
                        .flatMap(node -> node.history().events().stream())
                        .filter(event -> event.agent() == Agent.operator)
                        .map(History.Event::at)
                        .anyMatch(getLastDeployTime(entry.getKey())::isBefore))
                .map(Map.Entry::getKey)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Deploy in the maintenance thread to avoid scheduling multiple deployments of the same application if it takes
     * longer to deploy than the (short) maintenance interval of this
     */
    @Override
    protected void deploy(ApplicationId application) {
        deployWithLock(application);
        log.info("Redeployed application " + application.toShortString() +
                 " as a manual change was made to its nodes");
    }

}
