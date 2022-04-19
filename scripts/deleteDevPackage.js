const axios = require("axios");
const args = process.argv.slice(2);

const gitlabHost = args[0];
const gitlabToken = args[1];
const projectId = args[2];
const version = args[3];

const httpClient = axios.create({
  baseURL: `https://${gitlabHost}/api/v4/projects/${projectId}/`,
  headers: {
    "Content-Type": "application/json",
    "PRIVATE-TOKEN": gitlabToken,
  },
});

function searchPackages(packages) {
  for (const package of packages) {
    if (package.version === version) {
      return package;
    }
  }
}

function deletePackage(packageId) {
  return httpClient.delete(`packages/${packageId}`);
}

async function handlePackageDeletion(packages) {
  const package = searchPackages(packages);
  if (!package) {
    console.log("package not exist, no packages deleted");
    process.exit(0);
  }
  try {
    await deletePackage(package.id);
    console.log("Package deleted successfully");
    process.exit(0);
  } catch (error) {
    console.error("Error deleting package");
    console.error(error.message);
    process.exit(1);
  }
}

async function start() {
  try {
    const { data: packages } = await httpClient.get("packages?sort=desc");
    await handlePackageDeletion(packages);
  } catch (error) {
    console.log("Error fetching packages");
    console.log(error.message);
    process.exit(1);
  }
}

start();
