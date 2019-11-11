const google = require("googleapis");
exports.goWithTheDataFlow = function(req, res) {
  google.auth.getApplicationDefault(function(err, authClient) {
    if (err) {
      res.status(500).send("I failed you");
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
        console.error(`Problems getting projectId (${projectId}). Err was: `, err);
        res.status(500).send("I failed you");
        throw err;
      }
      const dataflow = google.dataflow({ version: "v1b3", auth: authClient });
      dataflow.projects.templates.create(
        {
          projectId: projectId,
          resource: {
            parameters: {
              inputDir: "gs://graph-input/datasets.imdbws.com/",
              showEntity: "showInfo",
              episodeEntity: "episodeInfo",
              searchEntity: "searchInfo",
              datastoreProject: "matsf-cloud-graph"
            },
            jobName:
              "called-from-a-cloud-function-batch-pipeline-" +
              new Date().getTime(),
            gcsPath: "gs://graph-backend/templates/imdbprocessor"
          }
        },
        function(err, response) {
          if (err) {
            console.error("Problem running dataflow template, error was: ",err);
            res.status(500).send("I failed you");
          } else {
            console.log("Dataflow template response: ", response);
            res.status(200).send("It worked");
          }
        }
      );
    });
  });
};
