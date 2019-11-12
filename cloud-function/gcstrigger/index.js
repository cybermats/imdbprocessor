const google = require("googleapis");
const {Storage} = require('@google-cloud/storage');


function goWithTheDataFlow() {
  google.auth.getApplicationDefault(function(err, authClient) {
    if (err) {
      console.error("It failed:", err);
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
            throw err;
          } else {
            console.log("Dataflow template response: ", response);
            return;
          }
        }
      );
    });
  });
};

async function getUpdateMinMax() {
    const storage = new Storage();
    const bucketName = 'graph-input-a';

    const [files] = await storage.bucket(bucketName).getFiles();
    updates = files.map(async (file) => {
        const [metadata] = await file.getMetadata();
        console.log("metadata.updated:", metadata.updated);
        return new Date(metadata.updated);
    });
    return Promise.all(updates).then(results => {
        let oldest = new Date('9999-01-01T00:00:00Z')
        let newest = new Date('0001-01-01T00:00:00Z')
        oldest = results.reduce((acc, curr) => {return acc < curr ? acc : curr;}, oldest);
        newest = results.reduce((acc, curr) => {return acc > curr ? acc : curr;}, newest);
        const duration = newest - oldest;
        console.log("data:", { oldest, newest, duration });
        return duration;
    });
}


exports.gcs_trigger_dataflow = (data, context) => {

    getUpdateMinMax().then(result => {
        console.log("Duration: ", result);
        const sixHours = 6 * 60 * 60 * 1000;
        if (result < sixHours) {
            console.log("Duration is less than 6 hours, starting dataflow job.");
            goWithTheDataFlow();
        }

    }).catch(error => {
        console.error("Something went wrong:", error);
        throw error;
    });
}