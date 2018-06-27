package com.yahoo.vespa.hosted.controller.restapi.application;

import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.JobType;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

public class RunHandlerHelperTest {

    @Test
    public void createJobTypeResponse() {
        List<JobType> jobArray = new ArrayList<>();
        jobArray.add(JobType.systemTest);
        jobArray.add(JobType.productionApNortheast1);
        jobArray.add(JobType.productionCdAwsUsEast1a);

        HttpResponse response = RunHandlerHelper.createJobTypeResponse(jobArray);
        compare(response, "{ something: 'hello'}");
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
