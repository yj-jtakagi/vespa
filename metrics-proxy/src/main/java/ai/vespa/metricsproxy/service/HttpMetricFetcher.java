/*
* Copyright 2019 Oath Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
 */

package ai.vespa.metricsproxy.service;

import com.yahoo.log.LogLevel;
import com.yahoo.yolean.Exceptions;
import org.apache.http.HttpVersion;
import org.apache.http.client.fluent.Request;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Logger;

/**
 * HTTP client to get metrics or health data from a service
 *
 * @author hmusum
 */
public abstract class HttpMetricFetcher {
    private final static Logger log = Logger.getLogger(HttpMetricFetcher.class.getPackage().getName());
    public final static String STATE_PATH = "/state/v1/";
    final static String METRICS_PATH = STATE_PATH + "metrics";
    final static String HEALTH_PATH = STATE_PATH + "health";
    // The call to apache will do 3 retries. As long as we check the services in series, we can't have this too high.
    public static int CONNECTION_TIMEOUT = 5000;
    private final static int SOCKET_TIMEOUT = 60000;
    private final URL url;
    protected final VespaService service;
    private final int connectionTimeout; // ms
    private final int socketTimeout; //ms


    /**
     * @param service The service to fetch metrics from
     * @param port    The port to use
     */
    HttpMetricFetcher(VespaService service, int port, String path) {
        this.service = service;
        this.connectionTimeout = CONNECTION_TIMEOUT;
        this.socketTimeout = SOCKET_TIMEOUT;

        String u = "http://localhost:" + port + path;
        try {
            this.url = new URL(u);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Malformed URL" + u);
        }
        log.log(LogLevel.DEBUG, "Fetching metrics from " + u + " with timeout " + connectionTimeout);
    }

    String getJson() throws IOException {
        log.log(LogLevel.DEBUG, "Connecting to url " + url + " for service '" + service + "'");
        return Request.Get(url.toString())
                .connectTimeout(connectionTimeout)
                .userAgent("MetricsProxy")
                .version(HttpVersion.HTTP_1_0)
                .socketTimeout(socketTimeout)
                .execute().returnContent().asString();
    }

    public String toString() {
        return this.getClass().getSimpleName() + " using " + url;
    }

    protected String errMsgNoResponse(IOException e) {
        return "Unable to get response from service '" + service + "': " +
                Exceptions.toMessageString(e);
    }

    void handleException(Exception e, String data, int timesFetched) {
        logMessage("Unable to parse json '" + data + "' for service '" + service + "': " +
                           Exceptions.toMessageString(e), timesFetched);
    }

    private void logMessage(String message, int timesFetched) {
        if (service.isAlive() && timesFetched > 5) {
            log.log(LogLevel.INFO, message);
        } else {
            log.log(LogLevel.DEBUG, message);
        }
    }

    protected void logMessageNoResponse(String message, int timesFetched) {
        if (timesFetched > 5) {
            log.log(LogLevel.WARNING, message);
        } else {
            log.log(LogLevel.INFO, message);
        }
    }
}
