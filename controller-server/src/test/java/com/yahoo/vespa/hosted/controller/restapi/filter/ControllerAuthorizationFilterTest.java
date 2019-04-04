// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.restapi.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.application.container.handler.Request;
import com.yahoo.config.provision.SystemName;
import com.yahoo.jdisc.http.HttpRequest.Method;
import com.yahoo.jdisc.http.filter.DiscFilterRequest;
import com.yahoo.vespa.hosted.controller.ControllerTester;
import com.yahoo.vespa.hosted.controller.restapi.ApplicationRequestToDiscFilterRequestWrapper;
import com.yahoo.vespa.hosted.controller.role.Role;
import com.yahoo.vespa.hosted.controller.role.RoleMembership;
import com.yahoo.vespa.hosted.controller.role.RolePrincipal;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.Principal;
import java.util.Optional;
import java.util.Set;

import static com.yahoo.container.jdisc.RequestHandlerTestDriver.MockResponseHandler;
import static com.yahoo.jdisc.http.HttpResponse.Status.FORBIDDEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author bjorncs
 * @author jonmv
 */
public class ControllerAuthorizationFilterTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void operator() {
        ControllerTester tester = new ControllerTester();
        ControllerAuthorizationFilter filter = createFilter(tester);
        RolePrincipal operatorPrincipal = new RolePrincipal() {
            @Override public RoleMembership roles() { return Role.hostedOperator.limitedTo(tester.controller().system()); }
            @Override public String getName() { return "operator"; }
        };
        assertIsAllowed(invokeFilter(filter, createRequest(Method.POST, "/zone/v2/path", operatorPrincipal)));
        assertIsAllowed(invokeFilter(filter, createRequest(Method.PUT, "/application/v4/user", operatorPrincipal)));
        assertIsAllowed(invokeFilter(filter, createRequest(Method.GET, "/zone/v1/path", operatorPrincipal)));
    }

    @Test
    public void unprivileged() {
        ControllerTester tester = new ControllerTester();
        RolePrincipal everyonePrincipal = new RolePrincipal() {
            @Override public RoleMembership roles() { return Role.everyone.limitedTo(tester.controller().system()); }
            @Override public String getName() { return "user"; }
        };
        ControllerAuthorizationFilter filter = createFilter(tester);
        assertIsForbidden(invokeFilter(filter, createRequest(Method.POST, "/zone/v2/path", everyonePrincipal)));
        assertIsAllowed(invokeFilter(filter, createRequest(Method.PUT, "/application/v4/user", everyonePrincipal)));
        assertIsAllowed(invokeFilter(filter, createRequest(Method.GET, "/zone/v1/path", everyonePrincipal)));
    }

    @Test
    public void unprivilegedInPublic() {
        ControllerTester tester = new ControllerTester();
        tester.zoneRegistry().setSystemName(SystemName.Public);
        RolePrincipal everyonePrincipal = new RolePrincipal() {
            @Override public RoleMembership roles() { return Role.everyone.limitedTo(tester.controller().system()); }
            @Override public String getName() { return "user"; }
        };
        ControllerAuthorizationFilter filter = createFilter(tester);
        assertIsForbidden(invokeFilter(filter, createRequest(Method.POST, "/zone/v2/path", everyonePrincipal)));
        assertIsForbidden(invokeFilter(filter, createRequest(Method.PUT, "/application/v4/user", everyonePrincipal)));
        assertIsAllowed(invokeFilter(filter, createRequest(Method.GET, "/zone/v1/path", everyonePrincipal)));
    }

    private static void assertIsAllowed(Optional<AuthorizationResponse> response) {
        assertFalse("Expected no response from filter, but got \"" +
                    response.map(r -> r.message + "\" (" + r.statusCode + ")").orElse(""),
                    response.isPresent());
    }

    private static void assertIsForbidden(Optional<AuthorizationResponse> response) {
        assertTrue("Expected a response from filter", response.isPresent());
        assertEquals("Invalid status code", FORBIDDEN, response.get().statusCode);
    }

    private static ControllerAuthorizationFilter createFilter(ControllerTester tester) {
        return new ControllerAuthorizationFilter(tester.controller().system(), Set.of("http://localhost"));
    }

    private static Optional<AuthorizationResponse> invokeFilter(ControllerAuthorizationFilter filter,
                                                                DiscFilterRequest request) {
        MockResponseHandler responseHandlerMock = new MockResponseHandler();
        filter.filter(request, responseHandlerMock);
        return Optional.ofNullable(responseHandlerMock.getResponse())
                       .map(response -> new AuthorizationResponse(response.getStatus(), getErrorMessage(responseHandlerMock)));
    }

    private static DiscFilterRequest createRequest(Method method, String path, Principal principal) {
        Request request = new Request(path, new byte[0], Request.Method.valueOf(method.name()), principal);
        return new ApplicationRequestToDiscFilterRequestWrapper(request);
    }

    private static String getErrorMessage(MockResponseHandler responseHandler) {
        try {
            return mapper.readTree(responseHandler.readAll()).get("message").asText();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static class AuthorizationResponse {
        final int statusCode;
        final String message;

        AuthorizationResponse(int statusCode, String message) {
            this.statusCode = statusCode;
            this.message = message;
        }
    }

}
