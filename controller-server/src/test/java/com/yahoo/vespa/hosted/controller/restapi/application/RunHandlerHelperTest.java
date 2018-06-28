package com.yahoo.vespa.hosted.controller.restapi.application;

import com.yahoo.config.provision.ApplicationId;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.vespa.hosted.controller.api.integration.configserver.PrepareResponse;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.JobType;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.RunId;
import com.yahoo.vespa.hosted.controller.deployment.RunDetails;
import com.yahoo.vespa.hosted.controller.deployment.RunStatus;
import com.yahoo.vespa.hosted.controller.deployment.Step;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.fail;

public class RunHandlerHelperTest {

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

        HttpResponse response = RunHandlerHelper.jobTypeResponse(jobList, jobMap, jobUrl);
        compare(response, "{  \n" +
                "   \"jobs\":[  \n" +
                "      {  \n" +
                "         \"system-test\":{  \n" +
                "            \"last\":{  \n" +
                "               \"result\":\"success\",\n" +
                "               \"start\":\"2018-06-27T10:12:35Z\",\n" +
                "               \"end\":\"2018-06-27T10:13:05Z\",\n" +
                "               \"details\":\"https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job/system-test/run/1\",\n" +
                "               \"id\":1,\n" +
                "               \"steps\":{  \n" +
                "                  \"deployInitialReal\":\"succeeded\",\n" +
                "                  \"installInitialReal\":\"succeeded\",\n" +
                "                  \"startTests\":\"succeeded\",\n" +
                "                  \"deployTester\":\"succeeded\",\n" +
                "                  \"storeData\":\"succeeded\",\n" +
                "                  \"installTester\":\"succeeded\",\n" +
                "                  \"deployReal\":\"succeeded\",\n" +
                "                  \"installReal\":\"succeeded\",\n" +
                "                  \"deactivateTester\":\"succeeded\",\n" +
                "                  \"deactivateReal\":\"succeeded\"\n" +
                "               }\n" +
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
                "               \"details\":\"https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job/production-ap-northeast-1/run/1\",\n" +
                "               \"id\":1,\n" +
                "               \"steps\":{  \n" +
                "                  \"deployInitialReal\":\"succeeded\",\n" +
                "                  \"installInitialReal\":\"succeeded\",\n" +
                "                  \"startTests\":\"succeeded\",\n" +
                "                  \"deployTester\":\"succeeded\",\n" +
                "                  \"storeData\":\"succeeded\",\n" +
                "                  \"installTester\":\"succeeded\",\n" +
                "                  \"deployReal\":\"succeeded\",\n" +
                "                  \"installReal\":\"succeeded\",\n" +
                "                  \"deactivateTester\":\"succeeded\",\n" +
                "                  \"deactivateReal\":\"succeeded\"\n" +
                "               }\n" +
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
                "               \"details\":\"https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job/production-us-west-1/run/1\",\n" +
                "               \"id\":1,\n" +
                "               \"steps\":{  \n" +
                "                  \"deployInitialReal\":\"succeeded\",\n" +
                "                  \"installInitialReal\":\"succeeded\",\n" +
                "                  \"startTests\":\"failed\",\n" +
                "                  \"deployTester\":\"succeeded\",\n" +
                "                  \"storeData\":\"unfinished\",\n" +
                "                  \"installTester\":\"succeeded\",\n" +
                "                  \"deployReal\":\"succeeded\",\n" +
                "                  \"installReal\":\"succeeded\",\n" +
                "                  \"deactivateTester\":\"unfinished\",\n" +
                "                  \"deactivateReal\":\"unfinished\"\n" +
                "               }\n" +
                "            },\n" +
                "            \"url\":\"https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job/production-us-west-1\"\n" +
                "         }\n" +
                "      }\n" +
                "   ]\n" +
                "}\n");
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

        HttpResponse response = RunHandlerHelper.runStatusResponse(statusMap, jobTypeUrl);
        compare(response, "{  \n" +
                "   \"1\":{  \n" +
                "      \"result\":\"success\",\n" +
                "      \"start\":\"2018-06-27T10:12:35Z\",\n" +
                "      \"end\":\"2018-06-27T10:13:19Z\",\n" +
                "      \"details\":\"https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job/systemtest/run/1\",\n" +
                "      \"id\":1,\n" +
                "      \"steps\":{  \n" +
                "         \"deployInitialReal\":\"succeeded\",\n" +
                "         \"installInitialReal\":\"succeeded\",\n" +
                "         \"startTests\":\"succeeded\",\n" +
                "         \"deployTester\":\"succeeded\",\n" +
                "         \"storeData\":\"succeeded\",\n" +
                "         \"installTester\":\"succeeded\",\n" +
                "         \"deployReal\":\"succeeded\",\n" +
                "         \"installReal\":\"succeeded\",\n" +
                "         \"deactivateTester\":\"succeeded\",\n" +
                "         \"deactivateReal\":\"succeeded\"\n" +
                "      }\n" +
                "   },\n" +
                "   \"2\":{  \n" +
                "      \"result\":\"testError\",\n" +
                "      \"start\":\"2018-06-27T10:12:35Z\",\n" +
                "      \"end\":\"2018-06-27T10:13:31Z\",\n" +
                "      \"details\":\"https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job/systemtest/run/2\",\n" +
                "      \"id\":2,\n" +
                "      \"steps\":{  \n" +
                "         \"deployInitialReal\":\"succeeded\",\n" +
                "         \"installInitialReal\":\"succeeded\",\n" +
                "         \"startTests\":\"unfinished\",\n" +
                "         \"deployTester\":\"unfinished\",\n" +
                "         \"storeData\":\"unfinished\",\n" +
                "         \"installTester\":\"unfinished\",\n" +
                "         \"deployReal\":\"succeeded\",\n" +
                "         \"installReal\":\"failed\",\n" +
                "         \"deactivateTester\":\"unfinished\",\n" +
                "         \"deactivateReal\":\"unfinished\"\n" +
                "      }\n" +
                "   },\n" +
                "   \"3\":{  \n" +
                "      \"result\":\"success\",\n" +
                "      \"start\":\"2018-06-27T10:12:35Z\",\n" +
                "      \"end\":\"2018-06-27T10:13:05Z\",\n" +
                "      \"details\":\"https://testserver.com/application/v4/tenant/sometenant/application/someapp/instance/usuallydefault/job/systemtest/run/3\",\n" +
                "      \"id\":3,\n" +
                "      \"steps\":{  \n" +
                "         \"deployInitialReal\":\"succeeded\",\n" +
                "         \"installInitialReal\":\"succeeded\",\n" +
                "         \"startTests\":\"succeeded\",\n" +
                "         \"deployTester\":\"succeeded\",\n" +
                "         \"storeData\":\"succeeded\",\n" +
                "         \"installTester\":\"succeeded\",\n" +
                "         \"deployReal\":\"succeeded\",\n" +
                "         \"installReal\":\"succeeded\",\n" +
                "         \"deactivateTester\":\"succeeded\",\n" +
                "         \"deactivateReal\":\"succeeded\"\n" +
                "      }\n" +
                "   }\n" +
                "}");
    }


    @Test
    public void runDetailsResponse() {
        PrepareResponse prepResponse = new PrepareResponse();

        RunDetails details = new RunDetails(prepResponse, "convergance log", "test log");
        HttpResponse response = RunHandlerHelper.runDetailsResponse(details);
        compare(response, "{\"testlog\":\"test log\",\"deployment\":{},\"convergenceLog\":\"convergance log\"}");
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
        if (lastStepStatus == Step.Status.failed || (lastStepStatus != Step.Status.unfinished && lastStep == Step.deactivateTester)) {
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
