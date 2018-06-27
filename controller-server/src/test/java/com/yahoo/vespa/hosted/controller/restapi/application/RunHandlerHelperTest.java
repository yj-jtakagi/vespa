package com.yahoo.vespa.hosted.controller.restapi.application;

import com.yahoo.config.provision.ApplicationId;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.JobType;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.RunId;
import com.yahoo.vespa.hosted.controller.deployment.RunStatus;
import com.yahoo.vespa.hosted.controller.deployment.Step;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedOutputStream;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
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
    public void createJobTypeResponse() {
        Map<JobType, RunStatus> jobMap = new HashMap<>();
        jobMap.put(JobType.systemTest, createStatus(JobType.systemTest, 1, 30, Step.installReal, Step.Status.succeeded));
        jobMap.put(JobType.productionApNortheast1, createStatus(JobType.productionApNortheast1, 1, 60, Step.last(), Step.Status.succeeded));
        jobMap.put(JobType.productionUsWest1, createStatus(JobType.productionUsWest1, 1, 60, Step.startTests, Step.Status.failed));


        HttpResponse response = RunHandlerHelper.createJobTypeResponse(jobMap);
        compare(response, "{ something: 'hello'}");
    }


    private RunStatus createStatus(JobType type, long runid, long duration, Step lastStep, Step.Status lastStepStatus) {
        RunId runId = new RunId(appId, type, 1l);

        // Initialise steps as unfinished
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
        } catch (IOException|JSONException e) {
            fail();
        }
    }
}
