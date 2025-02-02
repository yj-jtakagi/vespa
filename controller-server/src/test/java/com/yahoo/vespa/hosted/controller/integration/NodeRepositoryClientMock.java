// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.integration;

import com.yahoo.component.Version;
import com.yahoo.config.provision.ApplicationId;
import com.yahoo.config.provision.NodeType;
import com.yahoo.config.provision.zone.ZoneId;
import com.yahoo.vespa.hosted.controller.api.integration.configserver.NodeRepository;
import com.yahoo.vespa.hosted.controller.api.integration.noderepository.NodeList;
import com.yahoo.vespa.hosted.controller.api.integration.noderepository.NodeMembership;
import com.yahoo.vespa.hosted.controller.api.integration.noderepository.NodeOwner;
import com.yahoo.vespa.hosted.controller.api.integration.noderepository.NodeRepositoryNode;
import com.yahoo.vespa.hosted.controller.api.integration.noderepository.NodeState;

import java.util.Collection;
import java.util.List;

/**
 * @author bjorncs
 */
public class NodeRepositoryClientMock implements NodeRepository {

    @Override
    public void addNodes(ZoneId zone, Collection<NodeRepositoryNode> nodes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NodeRepositoryNode getNode(ZoneId zone, String hostname) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteNode(ZoneId zone, String hostname) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NodeList listNodes(ZoneId zone) {
        NodeRepositoryNode nodeA = createNodeA();
        NodeRepositoryNode nodeB = createNodeB();
        return new NodeList(List.of(nodeA, nodeB));
    }

    @Override
    public NodeList listNodes(ZoneId zone, ApplicationId application) {
        NodeRepositoryNode nodeA = createNodeA();
        NodeRepositoryNode nodeB = createNodeB();
        return new NodeList(List.of(nodeA, nodeB));
    }

    private static NodeRepositoryNode createNodeA() {
        NodeRepositoryNode node = new NodeRepositoryNode();
        node.setHostname("hostA");
        node.setCost(10);
        node.setFlavor("C-2B/24/500");
        node.setMinCpuCores(24d);
        node.setMinDiskAvailableGb(500d);
        node.setMinMainMemoryAvailableGb(24d);
        NodeOwner owner = new NodeOwner();
        owner.tenant = "tenant1";
        owner.application = "app1";
        owner.instance = "default";
        node.setOwner(owner);
        NodeMembership membership = new NodeMembership();
        membership.clusterid = "clusterA";
        membership.clustertype = "container";
        node.setMembership(membership);
        node.setState(NodeState.active);
        return node;
    }

    private static NodeRepositoryNode createNodeB() {
        NodeRepositoryNode node = new NodeRepositoryNode();
        node.setHostname("hostB");
        node.setCost(20);
        node.setFlavor("C-2C/24/500");
        node.setMinCpuCores(40d);
        node.setMinDiskAvailableGb(500d);
        node.setMinMainMemoryAvailableGb(24d);
        NodeOwner owner = new NodeOwner();
        owner.tenant = "tenant2";
        owner.application = "app2";
        owner.instance = "default";
        node.setOwner(owner);
        NodeMembership membership = new NodeMembership();
        membership.clusterid = "clusterB";
        membership.clustertype = "content";
        node.setMembership(membership);
        node.setState(NodeState.active);
        return node;
    }

    @Override
    public void setState(ZoneId zone, NodeState nodeState, String nodename) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void upgrade(ZoneId zone, NodeType type, Version version) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void upgradeOs(ZoneId zone, NodeType type, Version version) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void requestFirmwareCheck(ZoneId zone) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancelFirmwareCheck(ZoneId zone) {
        throw new UnsupportedOperationException();
    }

}
