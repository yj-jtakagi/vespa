package com.yahoo.vespa.hosted.controller.restapi.application;

import com.yahoo.config.provision.ApplicationId;
import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.slime.Cursor;
import com.yahoo.slime.Slime;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.JobType;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.RunId;
import com.yahoo.vespa.hosted.controller.application.ApplicationVersion;
import com.yahoo.vespa.hosted.controller.deployment.JobController;
import com.yahoo.vespa.hosted.controller.deployment.RunDetails;
import com.yahoo.vespa.hosted.controller.deployment.RunStatus;
import com.yahoo.vespa.hosted.controller.restapi.Path;
import com.yahoo.vespa.hosted.controller.restapi.SlimeJsonResponse;

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
     * @return Response with all job types that have recorded runs for the application and the status for the last run of that type
     */
    public static HttpResponse createJobTypeResponse(Map<JobType, RunStatus> jobTypesWithLastStatus) {
        Slime slime = new Slime();
        Cursor typesObject = slime.setObject();
        jobTypesWithLastStatus.forEach((jobType, runStatus) -> toSlime(typesObject.setObject(jobType.jobName()), runStatus));
        return new SlimeJsonResponse(slime);
    }

    /**
     * @return Response with the 10 last runs for a given jobtype
     * @see RunStatus
     */
    public static HttpResponse createRunStatusResponse(Map<RunId, RunStatus> runStatuses) {
        Slime slime = new Slime();
        Cursor runsObject = slime.setObject();

        //TODO sort on runid and limit to last 10
        runStatuses.forEach((runid, runstatus) -> toSlime(runsObject.setObject((int) runid.number()), runstatus));

        //TODO add links to details
        return new SlimeJsonResponse(slime);
    }

    private static void toSlime(Cursor runObject, RunStatus runStatus) {

        runObject.setLong("id", runStatus.id().number());
        runObject.setString("start", runStatus.start().toString());

        Cursor statusArray = runObject.addArray();
        runStatus.steps().forEach((step, status) -> {
            statusArray.setString(step.name(), status.name());
        });

        runStatus.end().ifPresent(instant -> runObject.setString("end", instant.toString()));
        runStatus.result().ifPresent(result -> runObject.setString("result", result.name()));
    }

    /**
     * @return Response with the details about a specific run
     * @see RunDetails
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
