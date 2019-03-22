/*
 * Copyright 2019 Oath Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
 */

package ai.vespa.metricsproxy.service;

import ai.vespa.metricsproxy.metric.HealthMetric;
import ai.vespa.metricsproxy.metric.Metrics;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * GVL TODO: Add service dimensions as a field and remove the static helper in VespaServices
 *
 * Represents a Vespa service
 *
 * @author Jo Kristian Bergum
 */
public class VespaService implements Comparable<VespaService> {
    private final String name;
    private final String id;

    private volatile int pid = -1;
    private volatile String state = "UNKNOWN";

    // Used to keep the last polled system metrics for service
    private Metrics systemMetrics;
    private String monitoringName; // monitoring-system-name.serviceName
    private final String serviceName;

    private final int statePort;

    private final RemoteHealthMetricFetcher remoteHealthMetricFetcher;
    private final RemoteMetricsFetcher remoteMetricsFetcher;

    private boolean isAlive;

    // Used to keep track of log level when health or metrics requests fail
    private AtomicInteger metricsFetchCount = new AtomicInteger(0);
    private AtomicInteger healthFetchCount = new AtomicInteger(0);


    public static VespaService create(String name, String id, int statePort) {
        String serviceName = name.replaceAll("\\d*$", "");
        return new VespaService(serviceName, name, id, statePort);
    }

    // GVL TODO: constructors are only used from tests. Try to keep them package-private.

    VespaService(String serviceName, String id) {
        this(serviceName, serviceName, id, -1);
    }

    VespaService(String serviceName, String name, String id) {
        this(serviceName, name, id, -1);
    }

    private VespaService(String serviceName, String name, String id, int statePort) {
        this.serviceName = serviceName;
        this.name = name;
        this.id = id;
        this.systemMetrics = new Metrics();
        this.statePort = statePort;
        this.isAlive = false;
        this.remoteMetricsFetcher = (this.statePort> 0) ? new RemoteMetricsFetcher(this, this.statePort) : new DummyMetricsFetcher(this);
        this.remoteHealthMetricFetcher = (this.statePort > 0) ? new RemoteHealthMetricFetcher(this, this.statePort) : new DummyHealthMetricFetcher(this);
    }

    public void setMonitoringName(String serviceName) {
        this.monitoringName = serviceName;
    }

    public String getMonitoringName() {
        if (this.monitoringName == null) {
            return "vespa." + serviceName;
        } else {
            return this.monitoringName;
        }
    }

    /**
     * Get the http port for state/v1
     *
     * @return health http port
     */
    int getStatePort() {
        return this.statePort;
    }

    /**
     * Returns true if the service has an http status port
     *
     * @return true if the services has an HTTP status port, false otherwise
     */
    boolean hasStatusPort() {
        return this.statePort > 0;
    }

    @Override
    public int compareTo(VespaService other) {
        return this.getInstanceName().compareTo(other.getInstanceName());
    }

    /**
     * Get the service name/type. E.g 'searchnode', but not 'searchnode2'
     *
     * @return the service name
     */
    public String getServiceName() {
        return this.serviceName;
    }

    /**
     * Get the instance name. E.g searchnode2
     *
     * @return the instance service name
     */
    public String getInstanceName() {
        return this.name;
    }

    /**
     * @return The health of this service
     */
    public HealthMetric getHealth() {
        HealthMetric healthMetric = remoteHealthMetricFetcher.getHealth(healthFetchCount.get());
        healthFetchCount.getAndIncrement();
        return healthMetric;
    }

    /**
     * Gets the system metrics for this service
     *
     * @return System metrics
     */
    public synchronized Metrics getSystemMetrics() {
        return this.systemMetrics;
    }

    /**
     * Get the Metrics registered for this service. Metrics are fetched over HTTP
     * if a metric http port has been defined, otherwise from log file
     *
     * @return the non-system metrics
     */
    public Metrics getMetrics() {
        Metrics remoteMetrics = remoteMetricsFetcher.getMetrics(metricsFetchCount.get());
        metricsFetchCount.getAndIncrement();
        return remoteMetrics;
    }

    /**
     * Gets the config id of this service
     *
     * @return the config id
     */
    public String getConfigId() {
        return id;
    }

    /**
     * The current pid of this service
     *
     * @return The pid
     */
    public int getPid() {
        return this.pid;
    }

    /**
     * update the pid of this service
     *
     * @param pid The pid that this service runs as
     */
    public void setPid(int pid) {
        this.pid = pid;
    }

    /**
     * Get the string representation of the state of this service
     *
     * @return string representing the state of this service - obtained from config-sentinel
     */
    public String getState() {
        return state;
    }

    /**
     * Update the state of this service
     *
     * @param state the new state
     */
    public void setState(String state) {
        this.state = state;
    }

    /**
     * Check if this pid/service is running
     *
     * @return true if the service is alive (e.g the pid is running)
     */
    public boolean isAlive() {
        return (isAlive && (pid >= 0));
    }

    @Override
    public String toString() {
        return name + ":" + pid + ":" + state + ":" + id;
    }

    public void setAlive(boolean alive) {
        this.isAlive = alive;
    }

    public synchronized void setSystemMetrics(Metrics systemMetrics) {
        this.systemMetrics = systemMetrics;
    }
}
