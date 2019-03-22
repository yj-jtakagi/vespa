/*
 * Copyright 2019 Oath Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
 */

package ai.vespa.metricsproxy.core;

import ai.vespa.metricsproxy.metric.ExternalMetrics;
import ai.vespa.metricsproxy.metric.VespaMetrics;
import ai.vespa.metricsproxy.metric.dimensions.ApplicationDimensions;
import ai.vespa.metricsproxy.metric.dimensions.NodeDimensions;
import ai.vespa.metricsproxy.metric.model.ConsumerId;
import ai.vespa.metricsproxy.metric.model.DimensionId;
import ai.vespa.metricsproxy.metric.model.MetricsPacket;
import ai.vespa.metricsproxy.service.VespaService;
import ai.vespa.metricsproxy.service.VespaServices;
import com.yahoo.component.Vtag;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static ai.vespa.metricsproxy.metric.ExternalMetrics.extractConfigserverDimensions;
import static ai.vespa.metricsproxy.metric.model.DimensionId.toDimensionId;
import static com.yahoo.log.LogLevel.DEBUG;
import static java.util.stream.Collectors.toList;

/**
 * TODO: Remove reference to VespaServices. Instead of service name, the methods here should take a service or list of services as input.
 *
 * @author gjoranv
 */
public class MetricsManager {
    private static Logger log = Logger.getLogger(MetricsManager.class.getName());

    static final DimensionId VESPA_VERSION = toDimensionId("vespaVersion");

    private final VespaServices vespaServices;
    private final VespaMetrics vespaMetrics;
    private final ExternalMetrics externalMetrics;
    private final MetricsConsumers metricsConsumers;
    private final ApplicationDimensions applicationDimensions;
    private final NodeDimensions nodeDimensions;

    private volatile Map<DimensionId, String> extraDimensions = new HashMap<>();

    public MetricsManager(VespaServices vespaServices, VespaMetrics vespaMetrics,
                          ExternalMetrics externalMetrics, MetricsConsumers metricsConsumers,
                          ApplicationDimensions applicationDimensions, NodeDimensions nodeDimensions) {
        this.vespaServices = vespaServices;
        this.vespaMetrics = vespaMetrics;
        this.externalMetrics = externalMetrics;
        this.metricsConsumers = metricsConsumers;
        this.applicationDimensions = applicationDimensions;
        this.nodeDimensions = nodeDimensions;
    }

    /**
     * Returns all metrics for the given service that are whitelisted for the given consumer.
     */
    public String getMetricNamesForServiceAndConsumer(String service, ConsumerId consumer) {
        return vespaMetrics.getMetricNames(vespaServices.getMonitoringServices(service), consumer);
    }

    public String getMetricsByConfigId(String configId) {
        return vespaMetrics.getMetricsAsString(vespaServices.getInstancesById(configId));
    }

    /**
     * Returns the metrics for the given services. The empty list is returned if no services are given.
     *
     * @param services The services to retrieve metrics for.
     * @return Metrics for all matching services.
     */
    public List<MetricsPacket> getMetrics(List<VespaService> services, Instant startTime) {
        if (services.isEmpty()) return Collections.emptyList();

        List<MetricsPacket.Builder> result = vespaMetrics.getMetrics(services);
        log.log(DEBUG, () -> "Got " + result.size() + " metrics packets for vespa services.");

        List<MetricsPacket.Builder> externalPackets = externalMetrics.getMetrics().stream()
                .filter(MetricsPacket.Builder::hasMetrics)
                .collect(toList());
        log.log(DEBUG, () -> "Got " + externalPackets.size() + " external metrics packets with whitelisted metrics.");

        result.addAll(externalPackets);

        return result.stream()
                .map(builder -> builder.putDimensionsIfAbsent(getAllGlobalDimensions()))
                .map(builder -> builder.putDimensionsIfAbsent(extraDimensions))
                .map(builder -> adjustTimestamp(builder, startTime))
                .map(MetricsPacket.Builder::build)
                .collect(Collectors.toList());
    }

    /**
     * GVL TODO: remove 'all' from name after unit test migration
     *
     * Returns a merged map of all global dimensions.
     */
    private Map<DimensionId, String> getAllGlobalDimensions() {
        Map<DimensionId, String> globalDimensions = new LinkedHashMap<>(applicationDimensions.getDimensions());
        globalDimensions.putAll(nodeDimensions.getDimensions());
        globalDimensions.put(VESPA_VERSION, Vtag.currentVersion.toFullString());
        return globalDimensions;
    }

    /**
     * If the metrics packet is less than one minute newer or older than the given startTime,
     * set its timestamp to the given startTime. This is done to ensure that metrics retrieved
     * from different sources for this invocation get the same timestamp, and a timestamp as close
     * as possible to the invocation from the external metrics retrieving client. The assumption
     * is that the client requests metrics periodically every minute.
     * <p>
     * However, if the timestamp of the packet is too far off in time, we don't adjust it because
     * we would otherwise be masking a real problem with retrieving the metrics.
     */
    static MetricsPacket.Builder adjustTimestamp(MetricsPacket.Builder builder, Instant startTime) {
        Duration age = Duration.between(startTime, builder.getTimestamp());
        if (age.abs().minusMinutes(1).isNegative())
            builder.timestamp(startTime.getEpochSecond());
        return builder;
    }

    /**
     * Returns the health metrics for the given services. The empty list is returned if no services are given.
     *
     * @param services The services to retrieve health metrics for.
     * @return Health metrics for all matching services.
     */
    public List<MetricsPacket> getHealthMetrics(List<VespaService> services) {
        if (services.isEmpty()) return Collections.emptyList();
        // TODO: Add global dimensions to health metrics?
        return vespaMetrics.getHealthMetrics(services);
    }

    public void setExtraMetrics(List<MetricsPacket.Builder> packets) {
        extraDimensions = extractConfigserverDimensions(packets);
        externalMetrics.setExtraMetrics(packets);
    }

    /**
     * Returns a space separated list of all distinct service names.
     */
    public String getAllVespaServices() {
        return vespaServices.getVespaServices().stream()
                .map(VespaService::getServiceName)
                .distinct()
                .collect(Collectors.joining(" "));
    }

}
