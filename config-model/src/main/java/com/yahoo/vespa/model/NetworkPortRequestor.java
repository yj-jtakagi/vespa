// Copyright 2019 Oath Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

package com.yahoo.vespa.model;

/**
 * Interface implemented by services using network ports, identifying its requirements.
 * @author arnej
 */
public interface NetworkPortRequestor {

    /** Returns the type of service */
    String getServiceType();

   /** Returns the name that identifies this service for the config-sentinel */
    String getServiceName();

    /** Returns the config id */
    String getConfigId();

    /**
     * Returns the desired base port for this service, or '0' if this
     * service should use the default port allocation mechanism.
     *
     * @return The desired base port for this service.
     */
    default int getWantedPort() { return 0; }

    /** Returns the number of ports needed by this service. */
    int getPortCount();

    /**
     * Returns true if the desired base port (returned by
     * getWantedPort()) for this service is the only allowed base
     * port.
     *
     * @return true if this Service requires the wanted base port.
     */
    default boolean requiresWantedPort() { return false; }

    /** allocate the ports you need */
    void allocatePorts(int start, NetworkPortAllocator from);
}
