// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.api.integration.stubs;

import com.yahoo.vespa.hosted.controller.api.application.v4.model.configserverbindings.ConfigChangeActions;
import com.yahoo.vespa.hosted.controller.api.application.v4.model.configserverbindings.RefeedAction;
import com.yahoo.vespa.hosted.controller.api.application.v4.model.configserverbindings.RestartAction;
import com.yahoo.vespa.hosted.controller.api.identifiers.TenantId;
import com.yahoo.vespa.hosted.controller.api.integration.LogStore;
import com.yahoo.vespa.hosted.controller.api.integration.configserver.PrepareResponse;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.RunId;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author freva
 */
public class MockLogStore implements LogStore {

    private Map<RunId, String> testLogs = new HashMap<>();
    private Map<RunId, String> convergenceLogs = new HashMap<>();
    private Map<RunId, PrepareResponse> prepareResponses = new HashMap<>();

    @Override
    public String getTestLog(RunId id) {
        return testLogs.isEmpty() ? "SUCCESS" : testLogs.get(id);
    }

    @Override
    public void setTestLog(RunId id, String testLog) {
        testLogs.put(id, testLog);
    }

    @Override
    public String getConvergenceLog(RunId id) {
        return convergenceLogs.isEmpty() ? "SUCCESS" : convergenceLogs.get(id);
    }

    @Override
    public void setConvergenceLog(RunId id, String convergenceLog) {
        convergenceLogs.put(id, convergenceLog);
    }

    @Override
    public PrepareResponse getPrepareResponse(RunId id) {
        return prepareResponses.isEmpty() ? createPrepareResponse(Collections.emptyList(), Collections.emptyList()) : prepareResponses.get(id);
    }

    @Override
    public void setPrepareResponse(RunId id, PrepareResponse prepareResponse) {
        prepareResponses.put(id, prepareResponse);
    }

    @Override
    public void deleteTestData(RunId id) {
        testLogs.remove(id);
        convergenceLogs.remove(id);
        prepareResponses.remove(id);
    }

    private static PrepareResponse createPrepareResponse(List<RestartAction> restartActions, List<RefeedAction> refeedActions) {
        PrepareResponse prepareResponse = new PrepareResponse();
        prepareResponse.message = "foo";
        prepareResponse.configChangeActions = new ConfigChangeActions(restartActions, refeedActions);
        prepareResponse.tenant = new TenantId("tenant");
        return prepareResponse;
    }
}
