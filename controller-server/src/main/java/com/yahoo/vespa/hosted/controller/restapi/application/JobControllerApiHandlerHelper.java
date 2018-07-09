package com.yahoo.vespa.hosted.controller.restapi.application;

import com.yahoo.config.provision.ApplicationId;
import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.slime.Cursor;
import com.yahoo.slime.Slime;
import com.yahoo.vespa.hosted.controller.api.application.v4.model.configserverbindings.RefeedAction;
import com.yahoo.vespa.hosted.controller.api.application.v4.model.configserverbindings.RestartAction;
import com.yahoo.vespa.hosted.controller.api.integration.LogStore;
import com.yahoo.vespa.hosted.controller.api.integration.configserver.PrepareResponse;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.JobType;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.RunId;
import com.yahoo.vespa.hosted.controller.application.ApplicationVersion;
import com.yahoo.vespa.hosted.controller.application.SourceRevision;
import com.yahoo.vespa.hosted.controller.deployment.JobController;
import com.yahoo.vespa.hosted.controller.deployment.RunStatus;
import com.yahoo.vespa.hosted.controller.restapi.Path;
import com.yahoo.vespa.hosted.controller.restapi.SlimeJsonResponse;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Implements the REST API for the job controller delegated from the Application API.
 *
 * @see JobController
 * @see ApplicationApiHandler
 */
public class JobControllerApiHandlerHelper {

    /**
     * @return Response with all job types that have recorded runs for the application _and_ the status for the last run of that type
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
     * @return Response with the runstatuses for a specific jobtype
     */
    public static HttpResponse runStatusResponse(Map<RunId, RunStatus> runStatuses, URI baseUriForJobType) {
        Slime slime = new Slime();
        Cursor cursor = slime.setObject();

        runStatuses.forEach((runid, runstatus) -> runStatusToSlime(cursor.setObject(Long.toString(runid.number())), runstatus, baseUriForJobType));

        return new SlimeJsonResponse(slime);
    }

    private static void runStatusToSlime(Cursor cursor, RunStatus runStatus, URI baseUriForJobType) {

        runStatus.result().ifPresent(result -> cursor.setString("result", result.name()));
        runStatus.end().ifPresent(instant -> cursor.setString("end", instant.toString()));

        Cursor stepsArray = cursor.setArray("steps");
        runStatus.steps().forEach((step, status) -> {
            Cursor stepObject = stepsArray.addObject();
            stepObject.setString(step.name(), status.name());
        });

        cursor.setString("start", runStatus.start().toString());
        cursor.setLong("id", runStatus.id().number());
        String logsPath = baseUriForJobType.getPath() + "/run/" + runStatus.id().number();
        cursor.setString("logs", baseUriForJobType.resolve(logsPath).toString());
    }

    /**
     * @return Response with logs from a single run
     */
    public static HttpResponse logStoreResponse(LogStore logStore, RunId runId) {
        Slime slime = new Slime();
        Cursor logsObject = slime.setObject();

        logsObject.setString("convergence", logStore.getConvergenceLog(runId));

        logsObject.setString("test", logStore.getTestLog(runId));

        Cursor deployCursor = logsObject.setObject("deploy");
        prepareResponseToSlime(deployCursor, logStore.getPrepareResponse(runId));

        return new SlimeJsonResponse(slime);
    }

    private static void prepareResponseToSlime(Cursor cursor, PrepareResponse prepareResponse) {

        cursor.setString("log", prepareResponse.log.stream()
                .map(a -> a.level + "\t" + a.time + "\t" + a.message)
                .collect(Collectors.joining("\n")));

        cursor.setString("message", prepareResponse.message);

        Cursor configCursor = cursor.setObject("config");

        // Refeed
        Cursor refeedCursor = configCursor.setArray("refeed");
        for (RefeedAction refeedAction : prepareResponse.configChangeActions.refeedActions) {
            Cursor refeedObject = refeedCursor.addObject();
            refeedObject.setString("clustername", refeedAction.clusterName);
            refeedObject.setString("documenttype", refeedAction.documentType);
            refeedObject.setString("name", refeedAction.name);
            refeedObject.setBool("allowed", refeedAction.allowed);
            refeedObject.setString("message", refeedAction.messages.stream().collect(Collectors.joining("\n")));
            refeedObject.setString("services", refeedAction.services.stream().map(s -> s.serviceName).collect(Collectors.joining("\n")));
        }

        // Restart
        Cursor restartCursor = configCursor.setArray("restart");
        for (RestartAction action : prepareResponse.configChangeActions.restartActions) {
            Cursor restartObject = restartCursor.addObject();
            restartObject.setString("clustername", action.clusterName);
            restartObject.setString("clustertype", action.clusterType);
            restartObject.setString("servicetype", action.serviceType);
            restartObject.setString("message", action.messages.stream().collect(Collectors.joining("\n")));
            restartObject.setString("services", action.services.stream().map(s -> s.serviceName).collect(Collectors.joining("\n")));
        }
    }

    /**
     * Unpack payload and submit to job controller. Defaults instance to 'default' and renders the
     * application version on success.
     *
     * @return Response with the new application version
     */
    public static HttpResponse submitResponse(JobController jobController, HttpRequest request) {

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
