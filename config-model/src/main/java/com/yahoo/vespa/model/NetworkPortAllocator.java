// Copyright 2019 Oath Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

package com.yahoo.vespa.model;

import java.util.ArrayList;
import java.util.List;

/**
 * API for allocating network ports
 * @author arnej
 */
public class NetworkPortAllocator {

    private HostPorts host;
    private NetworkPortRequestor service;
    private List<Integer> ports = new ArrayList<>();

    public NetworkPortAllocator(HostPorts host, NetworkPortRequestor service) {
        this.host = host;
        this.service = service;
    }

    public int requirePort(int port, String suffix) {
        int got = host.requireNetworkPort(port, service, suffix);
        ports.add(got);
        return got;
    }

    public int wantPort(int port, String suffix) {
        int got = host.wantNetworkPort(port, service, suffix);
        ports.add(got);
        return got;
    }

    public int allocatePort(String suffix) {
        int got = host.allocateNetworkPort(service, suffix);
        ports.add(got);
        return got;
    }

    public List<Integer> result() {
        return ports;
    }

}
