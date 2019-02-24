// Copyright 2019 Oath Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

package com.yahoo.vespa.model;

import com.yahoo.component.Version;
import com.yahoo.config.application.api.DeployLogger;
import com.yahoo.config.model.api.HostInfo;
import com.yahoo.config.provision.ClusterMembership;
import com.yahoo.config.provision.Flavor;
import com.yahoo.config.provision.NetworkPorts;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 *
 * @author arnej
 */
public class PortReservation {
    private int resolvedPort = -1;

    public int gotPort() {
        if (resolvedPort < 0) {
            throw new IllegalArgumentException("Cannot get port for "+this+", must be resolved first");
        }
        return resolvedPort;
    }

    public void resolve(int port) {
        if (resolvedPort >= 0) {
            throw new IllegalArgumentException("Cannot resolve port twice for "+this);
        }
        if (port < 0) {
            throw new IllegalArgumentException("Resolving to port "+port+" does not make sense for "+this);
        }
        this.resolvedPort = port;
    }

    final int wantedPort;
    final boolean requiresWantedPort;
    final NetworkPortRequestor service;
    final String suffix;

    PortReservation(NetworkPortRequestor svc, String suf) {
        this(0, false, svc, suf);
    }

    PortReservation(int port, NetworkPortRequestor svc, String suf) {
        this(port, false, svc, suf);
    }

    PortReservation(int port, boolean required, NetworkPortRequestor svc, String suf) {
        this.wantedPort = port;
        this.requiresWantedPort = required;
        this.service = svc;
        this.suffix = suf;
    }

    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("PortReservation[");
        if (wantedPort != 0) buf.append("want=").append(wantedPort).append(" ");
        if (requiresWantedPort) buf.append("required ");
        buf.append("service=").append(service.toString());
        buf.append(" suffix=").append(suffix);
        buf.append("]");
        return buf.toString();
   }

}
