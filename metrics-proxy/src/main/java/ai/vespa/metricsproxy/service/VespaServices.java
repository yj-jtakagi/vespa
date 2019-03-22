/*
 * Copyright 2019 Oath Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
 */

package ai.vespa.metricsproxy.service;

import ai.vespa.metricsproxy.metric.model.DimensionId;
import ai.vespa.metricsproxy.service.VespaServicesConfig.Service;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static ai.vespa.metricsproxy.core.MetricsConsumers.toUnmodifiableLinkedMap;
import static ai.vespa.metricsproxy.metric.model.DimensionId.toDimensionId;
import static com.yahoo.log.LogLevel.DEBUG;
import static java.util.Collections.emptyMap;

/**
 * Represents the Vespa services running on the node.
 *
 * @author gjoranv
 */
public class VespaServices {
    private static final Logger log = Logger.getLogger(VespaServices.class.getName());

    static final String SEPARATOR = ".";

    // All dimensions for each service. TODO: store in each service instead.
    private Map<String, Map<DimensionId, String>> serviceDimensions = emptyMap();

    private final ConfigSentinelClient sentinel;
    private final List<VespaService> services;

    @Inject
    public VespaServices(VespaServicesConfig config, MonitoringConfig monitoringConfig, ConfigSentinelClient sentinel) {
        this.services = createServices(config, monitoringConfig.systemName());
        this.sentinel = sentinel;
    }

    private List<VespaService> createServices(VespaServicesConfig servicesConfig, String monitoringSystem) {
        serviceDimensions = servicesConfig.service().stream().collect(
                toUnmodifiableLinkedMap(Service::id, VespaServices::getServiceDimensions));

        List<VespaService> services = new ArrayList<>();
        for (Service s : servicesConfig.service()) {
            log.log(DEBUG, "Re-configuring service " + s.name());
            VespaService vespaService = VespaService.create(s.name(), s.id(), s.healthport());
            vespaService.setMonitoringName(monitoringSystem + SEPARATOR + vespaService.getServiceName());
            services.add(vespaService);
        }
        log.log(DEBUG, "Created new services: " + services.size());
        updateServices(services);
        return services;
    }

    /**
     * Sets 'alive=false' for services that are no longer running.
     * Note that the status is updated in-place for the given services.
     */
    public void updateServices(List<VespaService> services) {
        if (sentinel != null) {
            log.log(DEBUG, "Updating services ");
            sentinel.updateServiceStatuses(services);
        }
    }

    /**
     * Get all known vespa services
     *
     * @return A list of VespaService objects
     */
    public List<VespaService> getVespaServices() {
        return Collections.unmodifiableList(services);
    }

    /**
     * @param id The configid
     * @return A list with size 1 as there should only be one service with the given configid
     */
    public List<VespaService> getInstancesById(String id) {
        List<VespaService> myServices = new ArrayList<>();
        for (VespaService s : services) {
            if (s.getConfigId().equals(id)) {
                myServices.add(s);
            }
        }

        return myServices;
    }

    /**
     * Get services matching pattern for the name used in the monitoring system.
     *
     * @param service name in monitoring system + service name, without index, e.g: vespa.container
     * @return A list of VespaServices
     */
    public List<VespaService> getMonitoringServices(String service) {
        if (service.equalsIgnoreCase("all"))
            return services;

        List<VespaService> myServices = new ArrayList<>();
        for (VespaService s : services) {
            log.log(DEBUG, () -> "getMonitoringServices. service=" + service + ", checking against " + s + ", which has monitoring name " + s.getMonitoringName());
            if (s.getMonitoringName().equalsIgnoreCase(service)) {
                myServices.add(s);
            }
        }

        return myServices;
    }

    // GVL TODO: Remove when dimensions are stored in each service.
    public synchronized Map<DimensionId, String> getServiceDimensions(String configid) {
        return serviceDimensions.get(configid);
    }

    private static Map<DimensionId, String> getServiceDimensions(Service service) {
        return service.dimension().stream().collect(
                toUnmodifiableLinkedMap(dim -> toDimensionId(dim.key()), Service.Dimension::value));
    }

}
