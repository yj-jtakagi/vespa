package com.yahoo.vespa.hosted.controller.restapi.application;

import com.yahoo.config.provision.ApplicationId;
import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;

import com.yahoo.slime.Cursor;
import com.yahoo.slime.Slime;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.JobType;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.RunId;
import com.yahoo.vespa.hosted.controller.application.ApplicationVersion;
import com.yahoo.vespa.hosted.controller.application.SourceRevision;
import com.yahoo.vespa.hosted.controller.deployment.DeploymentSteps;
import com.yahoo.vespa.hosted.controller.deployment.JobController;
import com.yahoo.vespa.hosted.controller.deployment.RunDetails;
import com.yahoo.vespa.hosted.controller.deployment.RunStatus;
import com.yahoo.vespa.hosted.controller.restapi.Path;
import com.yahoo.vespa.hosted.controller.restapi.SlimeJsonResponse;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    public static HttpResponse jobTypeResponse(List<JobType> sortedJobs, Map<JobType, RunStatus> lastStatus, URI baseUriForJobs) {
        Slime slime = new Slime();
        Cursor responseObject = slime.setObject();
        Cursor jobArray = responseObject.setArray("jobs");

        sortedJobs.forEach(jobType ->
                jobTypeToSlime(jobArray.addObject(), jobType, Optional.ofNullable(lastStatus.get(jobType)), baseUriForJobs));
        return new SlimeJsonResponse(slime);
    }

    private static void jobTypeToSlime(Cursor cursor, JobType jobType, Optional<RunStatus> runStatus, URI baseUriForJobs) {
        Cursor jobObject = cursor.setObject(jobType.jobName());

        // Url that are specific to the jobtype
        String jobTypePath = baseUriForJobs.getPath() + "/" + jobType.jobName();
        URI baseUriForJobType = baseUriForJobs.resolve(jobTypePath);
        jobObject.setString("url", baseUriForJobType.toString());

        // Add the last run status for the jobtype if present
        runStatus.ifPresent(status -> {
            Cursor lastObject = jobObject.setObject("last");
            runStatusToSlime(lastObject, status, baseUriForJobType);
        });
    }

    /**
     * @return Response with the 10 last runs for a given jobtype
     * @see RunStatus
     */
    public static HttpResponse runStatusResponse(Map<RunId, RunStatus> runStatuses, URI baseUriForJobType) {
        Slime slime = new Slime();
        Cursor cursor = slime.setObject();

        runStatuses.forEach((runid, runstatus) -> runStatusToSlime(cursor.setObject(Long.toString(runid.number())), runstatus, baseUriForJobType));

        //TODO add links to details
        return new SlimeJsonResponse(slime);
    }

    private static void runStatusToSlime(Cursor cursor, RunStatus runStatus, URI baseUriForJobType) {

        runStatus.result().ifPresent(result -> cursor.setString("result", result.name()));
        runStatus.end().ifPresent(instant -> cursor.setString("end", instant.toString()));

        Cursor statusObject = cursor.setObject("steps");
        runStatus.steps().forEach((step, status) -> {
            statusObject.setString(step.name(), status.name());
        });

        cursor.setString("start", runStatus.start().toString());
        cursor.setLong("id", runStatus.id().number());
        String detailsPath = baseUriForJobType.getPath() + "/run/" + runStatus.id().number();
        cursor.setString("details", baseUriForJobType.resolve(detailsPath).toString());
    }

    /**
     * @return Response with the details about a specific run
     * @see RunDetails
     */
    public static HttpResponse runDetailsResponse(RunDetails runDetails) {
        Slime slime = new Slime();
        Cursor jobDetailsObject = slime.setObject();

        // TODO complete this
        Cursor prepObject = jobDetailsObject.setObject("deployment");

        jobDetailsObject.setString("convergenceLog", runDetails.getConvergenceLog());
        jobDetailsObject.setString("testlog", runDetails.getTestLog());

        return new SlimeJsonResponse(slime);
    }

    /**
     * Unpack payload and submit to job controller. Defaults instance to 'default' and renders the
     * application version on success.
     *
     * @return Response with the new application version
     */
    public static HttpResponse submitAndRespond(JobController jobController, HttpRequest request) {

        Map<String, byte[]> dataParts = new MultipartParser().parse(request);

        byte[] applicationPackage = dataParts.get("applicationZip");
        byte[] applicationTestPackage = dataParts.get("applicationTestPackage");

        Path path = new Path(request.getUri().getPath());
        ApplicationId appId = ApplicationId.from(path.get("tenant"), path.get("application"), "default");


        SourceRevision sourceRevision = new SourceRevision("NA", "NA", "NA");
        String repo = request.getProperty("repository");
        String branch = request.getProperty("branch");
        String commit = request.getProperty("commit");
        if (repo != null && branch != null && commit != null) {
            sourceRevision = new SourceRevision(repo, branch, commit);
        }

        ApplicationVersion version = jobController.submit(appId, sourceRevision, applicationPackage, applicationTestPackage);

        Slime slime = new Slime();
        Cursor responseObject = slime.setObject();
        responseObject.setString("version", version.id());
        return new SlimeJsonResponse(slime);
    }
}
