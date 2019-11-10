const google = require("googleapis");
exports.goWithTheDataFlow = function(event, callback) {
  google.auth.getApplicationDefault(function(err, authClient) {
    if (err) {
      throw err;
    }
    // See https://cloud.google.com/compute/docs/authentication for more information on scopes
    if (authClient.createScopedRequired && authClient.createScopedRequired()) {
      // Scopes can be specified either as an array or as a single, space-delimited string.
      authClient = authClient.createScoped([
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/userinfo.email"
      ]);
    }
    google.auth.getDefaultProjectId(function(err, projectId) {
      if (err || !projectId) {
        console.error(
          `Problems getting projectId (${projectId}). Err was: `,
          err
        );
        throw err;
      }
      const dataflow = google.dataflow({ version: "v1b3", auth: authClient });
      dataflow.projects.templates.create(
        {
          projectId: projectId,
          resource: {
            parameters: {
              inputDir: "gs://graph-backend/input/",
              showEntity: "showInfo",
              episodeEntity: "episodeInfo",
              searchEntity: "searchInfo",
              datastoreProject: "matsf-cloud-graph"
            },
            jobName:
              "called-from-a-cloud-function-batch-pipeline-" +
              new Date().getTime(),
            gcsPath: "gs://graph-backend/template/imdbprocessor"
          }
        },
        function(err, response) {
          if (err) {
            console.error(
              "Problem running dataflow template, error was: ",
              err
            );
          }
          console.log("Dataflow template response: ", response);
          callback();
        }
      );
    });
  });
};
