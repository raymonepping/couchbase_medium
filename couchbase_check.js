const couchbase = require('couchbase');
require('dotenv').config();

async function main() {
  // Load connection details from the .env file
  const clusterConnStr = `couchbase://${process.env.COUCHBASE_ENDPOINT}`;
  const username = process.env.COUCHBASE_USERNAME;
  const password = process.env.COUCHBASE_PASSWORD;
  const bucketName = process.env.COUCHBASE_BUCKET;
  const scopeName = process.env.COUCHBASE_SCOPE_FTS;
  const collectionName = process.env.COUCHBASE_COLLECTION_FTS;
  const indexName = process.env.COUCHBASE_INDEX_FTS;

  try {
    // Connect to the Couchbase cluster
    const cluster = await couchbase.connect(clusterConnStr, {
      username: username,
      password: password,
    });

    // Get a reference to the specified bucket
    const bucket = cluster.bucket(bucketName);

    // Initialize the scope variable
    let scope;

    // Check for scope existence
    try {
      scope = bucket.scope(scopeName);
      console.log(`Scope "${scopeName}" exists.`);
    } catch (scopeErr) {
      console.error(`Scope "${scopeName}" does not exist.`);
      throw scopeErr;
    }

    // Check for collection existence within the scope
    try {
      scope.collection(collectionName);
      console.log(
        `Collection "${collectionName}" exists within scope "${scopeName}".`
      );
    } catch (collectionErr) {
      console.error(
        `Collection "${collectionName}" does not exist within scope "${scopeName}".`
      );
      throw collectionErr;
    }

    // Check for scoped index existence (example for Full Text Search index)
    try {
      const ftsManager = cluster.searchIndexes();
      const index = await ftsManager.getIndex(
        `${bucketName}.${scopeName}.${indexName}`
      );
      if (index) {
        console.log(`FTS Index "${indexName}" in scope "${scopeName}" exists.`);
      } else {
        console.error(
          `FTS Index "${indexName}" in scope "${scopeName}" does not exist.`
        );
        throw new Error(`FTS Index "${indexName}" not found`);
      }
    } catch (indexErr) {
      console.error(
        `Error retrieving FTS Index "${indexName}" in scope "${scopeName}":`,
        indexErr
      );
      throw indexErr;
    }

    console.log('Successful connection to Couchbase cluster!');
  } catch (err) {
    console.error('ERR:', err);
    process.exit(1);
  }
}

// Run the main function
main().then(() => process.exit(0));
