// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.container.jdisc.component;

import com.google.inject.Inject;


import java.util.Optional;
import java.util.logging.Logger;

import com.yahoo.log.LogLevel;

import java.util.List;
import java.util.Map;

import com.yahoo.component.ComponentId;
import com.yahoo.container.Container;
import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.container.jdisc.JdiscBindingsConfig;
import com.yahoo.container.jdisc.ThreadedHttpRequestHandler;
import com.yahoo.jdisc.handler.RequestHandler;
import com.yahoo.container.handler.VipStatus;


/**
 * Perform simplistic warmup using a static query request which aims to touch class loading
 *
 * Iterate over all configured server bindings which looks to have a search in the binding backed by a instance of ThreadedHttpRequestHandler
 *
 *
 * @author bergum
 */

public class Warmup {

    private static final Logger log = Logger.getLogger(Warmup.class.getName());

    /**
     * Number of warmup requests per handler
     */

    public static final int REQUESTS = 10;

    /**
     * Only warmup handlers with PATH_BINDING_PATTERN found in the endpoint binding
     */
    private static final String PATH_BINDING_PATTERN = "/search/";
    /**
     * The dummy static request we use to try do warm up
     */
    private static String SEARCH_STATIC_WARMUP_REQUEST =
            "/search/?yql=select%20%2A%20from%20sources%20%2A%20where%20sddocname%20contains%20%22foo%22%3B&hits=10&metrics.ignore";


    private JdiscBindingsConfig bindingsConfig;

    /**
     * @param status Injected VipStatus
     * @TODO we don't want to signal out of rotation on bindingsConfig changes or clientProviderRegistry changes
     * @TODO Need to be able to differentiate between fresh start and re-configuring
     */
    @Inject
    public Warmup(VipStatus status,
                  JdiscBindingsConfig bindingsConfig) {
        this(status,bindingsConfig,Container.get().getRequestHandlerRegistry().allComponentsById());

    }

    protected Warmup(VipStatus status,JdiscBindingsConfig bindingsConfig,Map<ComponentId, ? extends RequestHandler> handlersById) {
        this.bindingsConfig = bindingsConfig;
        log.info("Setting out of service before warmup");
        status.setInRotation(false);
        try {
            doWarmup(handlersById);
        } catch (Exception e) {
            e.printStackTrace();

        } catch (Error e) {
            e.printStackTrace();
        } finally {
            log.info("Setting back into rotation after warmup completed");
            status.setInRotation(true);
        }

    }

    /**
     * Find all endpoints powered by ThreadedRequestHandlers and warm them best effort
     *
     * @param handlersById set of configured request handlers
     */

    void doWarmup(Map<ComponentId, ? extends RequestHandler> handlersById) {
        for (Map.Entry<ComponentId, ? extends RequestHandler> handlerEntry : handlersById.entrySet()) {
            String id = handlerEntry.getKey().stringValue();
            RequestHandler handler = handlerEntry.getValue();
            if (handler instanceof ThreadedHttpRequestHandler) {
                ThreadedHttpRequestHandler httpHandler = (ThreadedHttpRequestHandler) handler;
                JdiscBindingsConfig.Handlers handlerConfig = bindingsConfig.handlers(id);
                if (handlerConfig != null) {
                    log.info("Considering warm up of  " + httpHandler.getClass().getName() + " for bindings" + handlerConfig.serverBindings());
                    warmUpEndpoint(httpHandler, getRequestPathFromBindings(handlerConfig.serverBindings()));
                }
            }
        }
    }

    /**
     * Check bindings against pattern, return "/"
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
     void warmUpEndpoint(ThreadedHttpRequestHandler handler, Optional<String> path) {
        if (path.isEmpty()) return;
        log.info("Warming up " + handler.getClass().getName() + " with request" + path.get());
        for (int i = 0; i < REQUESTS; i++) {
            try {
                HttpResponse response = handler.handle(HttpRequest.createTestRequest(path.get(),
                        com.yahoo.jdisc.http.HttpRequest.Method.GET));
                if (log.isLoggable(LogLevel.DEBUG)) {
                    log.log(LogLevel.DEBUG, "Got response for warmup request against path '"
                            + path.get() + "', response code was " + response.getStatus());
                }
            } catch (Exception e) {
            }
        }

    }
}
