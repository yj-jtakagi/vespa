package com.yahoo.vespa.hosted.controller.restapi.application;

import com.yahoo.config.provision.ApplicationId;
import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.slime.Cursor;
import com.yahoo.slime.Slime;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.JobType;
import com.yahoo.vespa.hosted.controller.application.ApplicationVersion;
import com.yahoo.vespa.hosted.controller.deployment.JobController;
import com.yahoo.vespa.hosted.controller.deployment.RunDetails;
import com.yahoo.vespa.hosted.controller.deployment.RunStatus;
import com.yahoo.vespa.hosted.controller.restapi.Path;
import com.yahoo.vespa.hosted.controller.restapi.SlimeJsonResponse;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Helper class that implements the REST API for the job controller. This API is part of the Application API.
 * <p>
 * A run is an instance of a @JobType that deploys and test the application package in one zone or test context (eg stage or prod.us-east-3)
 * <p>
 * A submission is a new application package accompanied by a test jar provided by a 3rd party system or from a user directly.
 * <p>
 *
 * @see JobController
 */
public class RunHandlerHelper {

    /**
     * @see JobType
     *
     * @return Response with all job types that have recorded runs for the application.
     */
    public static HttpResponse createJobTypeResponse(List<JobType> jobTypes) {
        Slime slime = new Slime();
        Cursor typesArray = slime.setArray();
        Arrays.stream(JobType.values()).forEach(jobType -> typesArray.setString(jobType.jobName(), jobType.jobName()));
        return new SlimeJsonResponse(slime);
    }

    /**
     * @see RunStatus
     *
     * @return Response with the 10 last runs for a given jobtype
     */
    public static HttpResponse createRunStatusResponse(List<RunStatus> runStatuses) {
        Slime slime = new Slime();
        Cursor runArray = slime.setArray();
        //TODO sort on last
        runStatuses.stream().limit(10).forEach(runStatus -> toSlime(runArray, runStatus));

        //TODO add links to details
        return new SlimeJsonResponse(slime);
    }

    private static void toSlime(Cursor jobArray, RunStatus runStatus) {
        Cursor jobObject = jobArray.addObject();
        jobObject.setLong("id", runStatus.id().number());
        jobObject.setString("start", runStatus.start().toString());

        Cursor statusArray = jobObject.addArray();
        runStatus.status().forEach((step, status) -> {
            statusArray.setString(step.name(), status.name());
        });

        runStatus.end().ifPresent(instant -> jobObject.setString("end", instant.toString()));
        runStatus.result().ifPresent(result -> jobObject.setString("result", result.name()));
    }

    /**
     * @see RunDetails
     *
     * @return Response with the details about a specific run
     */
    public static HttpResponse createRunDetailsResponse(RunDetails runDetails) {
        Slime slime = new Slime();
        Cursor jobDetailsObject = slime.setObject();

        jobDetailsObject.setString("preparelog", "Something prepare here");
        jobDetailsObject.setString("converganceLog", "Something convergance here");
        jobDetailsObject.setString("testlog", "Something test log");

        return new SlimeJsonResponse(slime);
    }

    /**
     * Unpack payload and submit to job controller. Defaults instance to 'default' and renders the
     * application version on success.
     *
     * @return Response with the new application version (TODO when available)
     */
    public static HttpResponse submitAndRespond(JobController jobController, HttpRequest request) {

        Map<String, byte[]> dataParts = new MultipartParser().parse(request);

        byte[] applicationPackage = dataParts.get("applicationZip");
        byte[] applicationTestPackage = dataParts.get("applicationTestPackage");

        Path path = new Path(request.getUri().getPath());
        ApplicationId appId = ApplicationId.from(path.get("tenant"), path.get("application"), "default");

        jobController.submit(appId, applicationPackage, applicationTestPackage);

        ApplicationVersion version = ApplicationVersion.unknown; // TODO get from jobcontroller

        Slime slime = new Slime();
        Cursor responseObject = slime.setObject();
        responseObject.setString("version", version.id());
        return new SlimeJsonResponse(slime);
    }
}
