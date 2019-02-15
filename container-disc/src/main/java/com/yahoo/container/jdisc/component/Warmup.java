// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.container.jdisc.component;

import com.google.inject.Inject;


import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import com.yahoo.component.provider.ComponentRegistry;
import com.yahoo.log.LogLevel;
import java.util.List;
import java.util.Map;
import com.yahoo.component.ComponentId;
import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.container.jdisc.JdiscBindingsConfig;
import com.yahoo.container.jdisc.ThreadedHttpRequestHandler;
import com.yahoo.jdisc.handler.RequestHandler;
import com.yahoo.container.handler.VipStatus;
import com.yahoo.yolean.Exceptions;



/**
 * Perform simplistic warmup using a static query request which aims to touch class loading
 * <p>
 * Iterate over all configured server bindings which looks to have a search in the binding backed by a instance of ThreadedHttpRequestHandler
 *
 * @author bergum
 */

public class Warmup {

    private static final Logger log = Logger.getLogger(Warmup.class.getName());
    private static final AtomicBoolean invoked = new AtomicBoolean(false);

    protected static void setInvoked(boolean value) {
        invoked.set(value);
    }


    /**
     * Number of warm up requests per handler
     */

    protected static final int REQUESTS = 1000;

    /**
     * Only warm up handlers with PATH_BINDING_PATTERN found in the endpoint binding
     */
    private static final String PATH_BINDING_PATTERN = "/search/";

    /**
     * The dummy static request we use to try do warm up
     */
    private static String SEARCH_STATIC_WARMUP_REQUEST =
            "/search/?yql=select%20%2A%20from%20sources%20%2A%20where%20sddocname%20contains%20%22foo%22%3B&hits=10&metrics.ignore";


    /**
     * @param status         Injected VipStatus
     * @param bindingsConfig Injected binding configuration
     */
    @Inject
    public Warmup(VipStatus status,
                  JdiscBindingsConfig bindingsConfig,
                  ComponentRegistry<RequestHandler> handlers
    ) {
        this(status, bindingsConfig, handlers.allComponentsById());
    }


    protected Warmup(VipStatus status, JdiscBindingsConfig bindingsConfig,
                     Map<ComponentId, ? extends RequestHandler> handlersById) {

        if (!invoked.compareAndSet(false, true)) {
            log.info("Was initialized but have performed warmup already so skipping.");
            return;
        }
        status.setInRotation(false);
        try {
            warmUpHandlers(handlersById, bindingsConfig);
        } catch (Error | Exception e) {
            log.warning("Caught and ignored error during warm up " + Exceptions.toMessageString(e));
        } finally {
            status.setInRotation(true);
        }
    }

    /**
     * Find all endpoints powered by ThreadedRequestHandlers and warm them best effort
     *
     * @param handlersById set of configured request handlers
     */

    void warmUpHandlers(Map<ComponentId, ? extends RequestHandler> handlersById, JdiscBindingsConfig bindingsConfig) {
        for (var handlerEntry : handlersById.entrySet()) {
            String id = handlerEntry.getKey().stringValue();
            RequestHandler handler = handlerEntry.getValue();
            if (handler instanceof ThreadedHttpRequestHandler) {
                ThreadedHttpRequestHandler httpHandler = (ThreadedHttpRequestHandler) handler;
                JdiscBindingsConfig.Handlers handlerConfig = bindingsConfig.handlers(id);
                if (handlerConfig != null) {
                    if (log.isLoggable(LogLevel.DEBUG)) {
                        log.log(LogLevel.DEBUG, "Considering warm up of  "
                                + httpHandler.getClass().getName() + " for bindings: " + handlerConfig.serverBindings());
                    }
                    warmUpHandler(httpHandler,
                            getRequestPathFromBindings(handlerConfig.serverBindings()));
                }
            }
        }
    }

    /**
     * Check if binding contains PATH_BINDING_PATTERN
     *
     * @param bindings list of bindings
     * @return Optional.empty if not a valid request point was found
     */
    Optional<String> getRequestPathFromBindings(List<String> bindings) {
        for (String binding : bindings) {
            if (binding.contains(PATH_BINDING_PATTERN)) {
                return Optional.of(SEARCH_STATIC_WARMUP_REQUEST);
            }
        }
        return Optional.empty();
    }


    /**
     * @param handler the handler to warm
     * @param path    the endpoint path, e.g /search/?stuff
     */
    void warmUpHandler(ThreadedHttpRequestHandler handler, Optional<String> path) {
        if (path.isEmpty()) return;
        log.info("Warming up " + handler.getClass().getName() + " with request '" + path.get() + "'");
        ExecutorService service = Executors.newFixedThreadPool(4);
        for (int i = 0; i < REQUESTS; i++) {
            try {
                service.execute( () -> {
                    HttpResponse response = handler.handle(HttpRequest.createTestRequest(path.get(),
                            com.yahoo.jdisc.http.HttpRequest.Method.GET));
                    if (log.isLoggable(LogLevel.DEBUG)) {
                        log.log(LogLevel.DEBUG, "Got response for warmup request against path '"
                                + path.get() + "', response code was " + response.getStatus());
                    }

                });
            } catch (Throwable e) {
                log.warning("Ignored Throwable during warm up: ' " + Exceptions.toMessageString(e));
            }
        }
        try {
            service.shutdown();
            service.awaitTermination(100, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warning("Issues during executor service shutdown " + Exceptions.toMessageString(e));
        }
    }
}
