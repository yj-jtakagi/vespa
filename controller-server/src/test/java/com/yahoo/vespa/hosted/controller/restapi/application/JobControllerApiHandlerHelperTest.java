package com.yahoo.vespa.hosted.controller.restapi.application;

import com.yahoo.config.provision.ApplicationId;
import com.yahoo.config.provision.TenantName;
import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.processing.Request;
import com.yahoo.vespa.athenz.api.AthenzDomain;
import com.yahoo.vespa.athenz.api.NToken;
import com.yahoo.vespa.hosted.controller.Controller;
import com.yahoo.vespa.hosted.controller.ControllerTester;
import com.yahoo.vespa.hosted.controller.api.application.v4.model.configserverbindings.ConfigChangeActions;
import com.yahoo.vespa.hosted.controller.api.application.v4.model.configserverbindings.RefeedAction;
import com.yahoo.vespa.hosted.controller.api.application.v4.model.configserverbindings.RestartAction;
import com.yahoo.vespa.hosted.controller.api.application.v4.model.configserverbindings.ServiceInfo;
import com.yahoo.vespa.hosted.controller.api.identifiers.Property;
import com.yahoo.vespa.hosted.controller.api.identifiers.TenantId;
import com.yahoo.vespa.hosted.controller.api.integration.LogStore;
import com.yahoo.vespa.hosted.controller.api.integration.configserver.Log;
import com.yahoo.vespa.hosted.controller.api.integration.configserver.PrepareResponse;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.JobType;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.RunId;
import com.yahoo.vespa.hosted.controller.api.integration.stubs.MockLogStore;
import com.yahoo.vespa.hosted.controller.deployment.JobController;
import com.yahoo.vespa.hosted.controller.deployment.RunStatus;
import com.yahoo.vespa.hosted.controller.deployment.Step;
import com.yahoo.vespa.hosted.controller.restapi.Path;
import com.yahoo.vespa.hosted.controller.tenant.AthenzTenant;
import com.yahoo.vespa.hosted.controller.tenant.UserTenant;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.fail;

public class JobControllerApiHandlerHelperTest {

    private final ApplicationId appId = ApplicationId.from("vespa", "music", "default");
    private final Instant start = Instant.parse("2018-06-27T10:12:35Z");

    @Test
    public void jobTypeResponse() {
        Map<JobType, RunStatus> jobMap = new HashMap<>();
        List<JobType> jobList = new ArrayList<>();
        jobMap.put(JobType.systemTest, createStatus(JobType.systemTest, 1, 30, Step.last(), Step.Status.succeeded));
        jobList.add(JobType.systemTest);
        jobMap.put(JobType.productionApNortheast1, createStatus(JobType.productionApNortheast1, 1, 60, Step.last(), Step.Status.succeeded));
        jobList.add(JobType.productionApNortheast1);
        jobMap.put(JobType.productionUsWest1, createStatus(JobType.productionUsWest1, 1, 60, Step.startTests, Step.Status.failed));
        jobList.add(JobType.productionUsWest1);

        URI jobUrl = URI.create("https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job");

        HttpResponse response = JobControllerApiHandlerHelper.jobTypeResponse(jobList, jobMap, jobUrl);
        compare(response, "{  \n" +
                "   \"jobs\":[  \n" +
                "      {  \n" +
                "         \"system-test\":{  \n" +
                "            \"last\":{  \n" +
                "               \"result\":\"success\",\n" +
                "               \"start\":\"2018-06-27T10:12:35Z\",\n" +
                "               \"end\":\"2018-06-27T10:13:05Z\",\n" +
                "               \"id\":1,\n" +
                "               \"steps\":[  \n" +
                "                  {  \n" +
                "                     \"deployInitialReal\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"installInitialReal\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"deployReal\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"installReal\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"deployTester\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"installTester\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"startTests\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"storeData\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"deactivateReal\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"deactivateTester\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"report\":\"succeeded\"\n" +
                "                  }\n" +
                "               ],\n" +
                "               \"logs\":\"https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job/system-test/run/1\"\n" +
                "            },\n" +
                "            \"url\":\"https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job/system-test\"\n" +
                "         }\n" +
                "      },\n" +
                "      {  \n" +
                "         \"production-ap-northeast-1\":{  \n" +
                "            \"last\":{  \n" +
                "               \"result\":\"success\",\n" +
                "               \"start\":\"2018-06-27T10:12:35Z\",\n" +
                "               \"end\":\"2018-06-27T10:13:35Z\",\n" +
                "               \"id\":1,\n" +
                "               \"steps\":[  \n" +
                "                  {  \n" +
                "                     \"deployInitialReal\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"installInitialReal\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"deployReal\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"installReal\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"deployTester\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"installTester\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"startTests\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"storeData\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"deactivateReal\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"deactivateTester\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"report\":\"succeeded\"\n" +
                "                  }\n" +
                "               ],\n" +
                "               \"logs\":\"https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job/production-ap-northeast-1/run/1\"\n" +
                "            },\n" +
                "            \"url\":\"https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job/production-ap-northeast-1\"\n" +
                "         }\n" +
                "      },\n" +
                "      {  \n" +
                "         \"production-us-west-1\":{  \n" +
                "            \"last\":{  \n" +
                "               \"result\":\"testError\",\n" +
                "               \"start\":\"2018-06-27T10:12:35Z\",\n" +
                "               \"end\":\"2018-06-27T10:13:35Z\",\n" +
                "               \"id\":1,\n" +
                "               \"steps\":[  \n" +
                "                  {  \n" +
                "                     \"deployInitialReal\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"installInitialReal\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"deployReal\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"installReal\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"deployTester\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"installTester\":\"succeeded\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"startTests\":\"failed\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"storeData\":\"unfinished\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"deactivateReal\":\"unfinished\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"deactivateTester\":\"unfinished\"\n" +
                "                  },\n" +
                "                  {  \n" +
                "                     \"report\":\"unfinished\"\n" +
                "                  }\n" +
                "               ],\n" +
                "               \"logs\":\"https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job/production-us-west-1/run/1\"\n" +
                "            },\n" +
                "            \"url\":\"https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job/production-us-west-1\"\n" +
                "         }\n" +
                "      }\n" +
                "   ]\n" +
                "}");
    }

    @Test
    public void runStatusResponse() {
        Map<RunId, RunStatus> statusMap = new HashMap<>();
        RunStatus status = null;

        status = createStatus(JobType.systemTest, 3, 30, Step.last(), Step.Status.succeeded);
        statusMap.put(status.id(), status);

        status = createStatus(JobType.systemTest, 2, 56, Step.installReal, Step.Status.failed);
        statusMap.put(status.id(), status);

        status = createStatus(JobType.systemTest, 1, 44, Step.last(), Step.Status.succeeded);
        statusMap.put(status.id(), status);

        URI jobTypeUrl = URI.create("https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job/systemtest");

        HttpResponse response = JobControllerApiHandlerHelper.runStatusResponse(statusMap, jobTypeUrl);
        compare(response, "{  \n" +
                "   \"1\":{  \n" +
                "      \"result\":\"success\",\n" +
                "      \"start\":\"2018-06-27T10:12:35Z\",\n" +
                "      \"end\":\"2018-06-27T10:13:19Z\",\n" +
                "      \"id\":1,\n" +
                "      \"steps\":[  \n" +
                "         {  \n" +
                "            \"deployInitialReal\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"installInitialReal\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"deployReal\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"installReal\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"deployTester\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"installTester\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"startTests\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"storeData\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"deactivateReal\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"deactivateTester\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"report\":\"succeeded\"\n" +
                "         }\n" +
                "      ],\n" +
                "      \"logs\":\"https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job/systemtest/run/1\"\n" +
                "   },\n" +
                "   \"2\":{  \n" +
                "      \"result\":\"testError\",\n" +
                "      \"start\":\"2018-06-27T10:12:35Z\",\n" +
                "      \"end\":\"2018-06-27T10:13:31Z\",\n" +
                "      \"id\":2,\n" +
                "      \"steps\":[  \n" +
                "         {  \n" +
                "            \"deployInitialReal\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"installInitialReal\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"deployReal\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"installReal\":\"failed\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"deployTester\":\"unfinished\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"installTester\":\"unfinished\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"startTests\":\"unfinished\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"storeData\":\"unfinished\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"deactivateReal\":\"unfinished\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"deactivateTester\":\"unfinished\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"report\":\"unfinished\"\n" +
                "         }\n" +
                "      ],\n" +
                "      \"logs\":\"https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job/systemtest/run/2\"\n" +
                "   },\n" +
                "   \"3\":{  \n" +
                "      \"result\":\"success\",\n" +
                "      \"start\":\"2018-06-27T10:12:35Z\",\n" +
                "      \"end\":\"2018-06-27T10:13:05Z\",\n" +
                "      \"id\":3,\n" +
                "      \"steps\":[  \n" +
                "         {  \n" +
                "            \"deployInitialReal\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"installInitialReal\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"deployReal\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"installReal\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"deployTester\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"installTester\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"startTests\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"storeData\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"deactivateReal\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"deactivateTester\":\"succeeded\"\n" +
                "         },\n" +
                "         {  \n" +
                "            \"report\":\"succeeded\"\n" +
                "         }\n" +
                "      ],\n" +
                "      \"logs\":\"https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job/systemtest/run/3\"\n" +
                "   }\n" +
                "}");
    }


    @Test
    public void logStoreResponse() {
        MockLogStore logStore = new MockLogStore();
        RunId runId = new RunId(appId, JobType.systemTest, 42);
        ServiceInfo serviceInfo = new ServiceInfo("servicename", "servicetype", "configid", "hostname");
        RestartAction restartAction = new RestartAction("custername", "clustertype", "servicetype", Collections.singletonList(serviceInfo), Collections.singletonList("message"));
        RefeedAction refeedAction = new RefeedAction("name", true, "documenttpye", "clustername", Collections.singletonList(serviceInfo), Collections.singletonList("message"));
        addLog(logStore, runId, "SUCCESS", "SUCCESS", "All clear", Collections.singletonList(restartAction), Collections.singletonList(refeedAction));
        HttpResponse response = JobControllerApiHandlerHelper.logStoreResponse(logStore, runId);
        compare(response, "{  \n" +
                "   \"test\":\"SUCCESS\",\n" +
                "   \"convergence\":\"SUCCESS\",\n" +
                "   \"deploy\":{  \n" +
                "      \"log\":\"info\\t1531143529\\tmessage1\\ninfo\\t1531143530\\tmessage2\",\n" +
                "      \"message\":\"All clear\",\n" +
                "      \"config\":{  \n" +
                "         \"restart\":[  \n" +
                "            {  \n" +
                "               \"clustertype\":\"clustertype\",\n" +
                "               \"servicetype\":\"servicetype\",\n" +
                "               \"clustername\":\"custername\",\n" +
                "               \"services\":\"servicename\",\n" +
                "               \"message\":\"message\"\n" +
                "            }\n" +
                "         ],\n" +
                "         \"refeed\":[  \n" +
                "            {  \n" +
                "               \"clustername\":\"clustername\",\n" +
                "               \"allowed\":true,\n" +
                "               \"name\":\"name\",\n" +
                "               \"documenttype\":\"documenttpye\",\n" +
                "               \"services\":\"servicename\",\n" +
                "               \"message\":\"message\"\n" +
                "            }\n" +
                "         ]\n" +
                "      }\n" +
                "   }\n" +
                "}\n");
    }

    @Test
    public void submitResponse() {
        ControllerTester tester = new ControllerTester();
        tester.createTenant("tenant", "domain", 1l);
        tester.createApplication(TenantName.from("tenant"), "application", "default", 1l);

        JobController jobController = new JobController(tester.controller(), new MockLogStore());
        jobController.register(ApplicationId.from("tenant", "application", "default"));

        HttpResponse response = JobControllerApiHandlerHelper.submitResponse(jobController, "tenant", "application", Collections.emptyMap(), new byte[0], new byte[0]);
        compare(response, "{\"version\":\"1.0.1-NA\"}");
    }


    private void addLog(LogStore logStore, RunId runId, String convLog, String testLog, String prepMessage, List<RestartAction> restartActions, List<RefeedAction> refeedActions) {
        logStore.setConvergenceLog(runId, convLog);
        logStore.setTestLog(runId, testLog);
        PrepareResponse prepareResponse = new PrepareResponse();
        prepareResponse.message = prepMessage;
        prepareResponse.log = new ArrayList<>();

        Log log1 = new Log();
        log1.message = "message1";
        log1.time = 1531143529;
        log1.level = "info";
        prepareResponse.log.add(log1);

        Log log2 = new Log();
        log2.message = "message2";
        log2.time = 1531143530;
        log2.level = "info";

        prepareResponse.log.add(log2);
        prepareResponse.configChangeActions = new ConfigChangeActions(restartActions, refeedActions);
        prepareResponse.tenant = new TenantId("tenant");
        logStore.setPrepareResponse(runId, prepareResponse);
    }

    private RunStatus createStatus(JobType type, long runid, long duration, Step lastStep, Step.Status lastStepStatus) {
        RunId runId = new RunId(appId, type, runid);

        Map<Step, Step.Status> stepStatusMap = new HashMap<>();
        Arrays.stream(Step.values()).sorted(Comparator.naturalOrder()).forEach(step -> {
            if (step.ordinal() < lastStep.ordinal()) {
                stepStatusMap.put(step, Step.Status.succeeded);
            } else if (step.equals(lastStep)) {
                stepStatusMap.put(step, lastStepStatus);
            } else {
                stepStatusMap.put(step, Step.Status.unfinished);
            }
        });

        Optional<Instant> end = Optional.empty();
        if (lastStepStatus == Step.Status.failed || (lastStepStatus != Step.Status.unfinished && lastStep == Step.last())) {
            end = Optional.of(start.plusSeconds(duration));
        }

        return new RunStatus(runId, stepStatusMap, start, end);
    }

    private void compare(HttpResponse response, String expected) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            response.render(baos);
            String actual = new String(baos.toByteArray());

            JSONObject actualJSON = new JSONObject(actual);
            JSONObject expectedJSON = new JSONObject(expected);
            Assert.assertEquals(expectedJSON.toString(), actualJSON.toString());
        } catch (IOException | JSONException e) {
            fail();
        }
    }
}
