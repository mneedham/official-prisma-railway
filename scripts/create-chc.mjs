import fs from "node:fs";

const log = {
  info: (msg) => console.log(`• ${msg}`),
  success: (msg) => console.log(`✓ ${msg}`),
  error: (msg) => console.error(`✗ ${msg}`),
  section: (msg) => console.log(`\n${msg}:`),
};

const envPath = ".env";

// Check if DATABASE_URL already exists
if (process.env.CLICKHOUSE_HOST) {
  log.info("ClickHouse connection already configured");
  process.exit(0);
}

// Check for existing .env unless --force is used
if (fs.existsSync(envPath) && !process.argv.includes("--force")) {
  log.info("Using existing .env (--force to regenerate)");
  process.exit(0);
}

// Validate required environment variables
const CLICKHOUSE_CLOUD_KEY = process.env.CLICKHOUSE_CLOUD_KEY;
const CLICKHOUSE_CLOUD_SECRET = process.env.CLICKHOUSE_CLOUD_SECRET;
const CLICKHOUSE_ORG_ID = process.env.CLICKHOUSE_ORG_ID;

if (!CLICKHOUSE_CLOUD_KEY || !CLICKHOUSE_CLOUD_SECRET || !CLICKHOUSE_ORG_ID) {
  log.error("Missing required environment variables:");
  log.error("  CLICKHOUSE_CLOUD_KEY");
  log.error("  CLICKHOUSE_CLOUD_SECRET");
  log.error("  CLICKHOUSE_ORG_ID");
  log.info("\nGet these from: https://clickhouse.cloud/");
  process.exit(1);
}

const API_BASE = "https://api.clickhouse.cloud/v1";

// Create Basic Auth header
const authHeader = `Basic ${Buffer.from(`${CLICKHOUSE_CLOUD_KEY}:${CLICKHOUSE_CLOUD_SECRET}`).toString("base64")}`;

async function createClickHouseService() {
  log.section("Provisioning ClickHouse Cloud service");

  try {
    // Create a new service
    const createResponse = await fetch(
      `${API_BASE}/organizations/${CLICKHOUSE_ORG_ID}/services`,
      {
        method: "POST",
        headers: {
          Authorization: authHeader,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          name: `railway-${Date.now()}`,
          provider: "aws",
          region: "us-east-1",
          idleScaling: true,
          idleTimeoutMinutes: 5,
          ipAccessList: [
            {
              source: "0.0.0.0/0",
              description: "Allow all (development)",
            },
          ],
        }),
      }
    );

    if (!createResponse.ok) {
      const errorText = await createResponse.text();
      throw new Error(
        `Failed to create service: ${createResponse.status} ${errorText}`
      );
    }

    const service = await createResponse.json();
    
    // Extract service data, password, and endpoints from response
    const serviceData = service.result.service;
    const password = service.result.password;
    const serviceId = serviceData.id;
    const serviceName = serviceData.name;
    const endpoints = serviceData.endpoints;
    
    if (!serviceId) {
      throw new Error(`Failed to get service ID from response: ${JSON.stringify(service)}`);
    }
    
    log.success(`Created service: ${serviceName} (${serviceId})`);

    // Wait for service to be ready
    log.info("Waiting for service to be ready...");
    let attempts = 0;
    const maxAttempts = 60; // 5 minutes

    while (attempts < maxAttempts) {
      const statusResponse = await fetch(
        `${API_BASE}/organizations/${CLICKHOUSE_ORG_ID}/services/${serviceId}`,
        {
          method: "GET",
          headers: {
            Authorization: authHeader,
          },
        }
      );

      if (!statusResponse.ok) {
        throw new Error(`Failed to check service status: ${statusResponse.status}`);
      }

      const statusData = await statusResponse.json();
      const state = statusData.result.state;
      
      log.info(`Current state: ${state}`);

      // Service is ready when it's running or idle
      if (state === "running" || state === "idle") {
        log.success("Service is ready");
        break;
      }

      // Check for terminal failure states
      if (state === "stopped" || state === "stopping") {
        throw new Error(`Service provisioning failed: state is ${state}`);
      }

      // Continue waiting for "starting" state
      attempts++;
      await new Promise((resolve) => setTimeout(resolve, 5000)); // Wait 5 seconds
    }

    if (attempts >= maxAttempts) {
      throw new Error("Service provisioning timed out");
    }

    // Build connection details (we already have endpoints and password from create response)
    const httpsEndpoint = endpoints.find((e) => e.protocol === "https");
    const nativeEndpoint = endpoints.find((e) => e.protocol === "nativesecure");

    const host = httpsEndpoint?.host || nativeEndpoint?.host;
    const httpsPort = httpsEndpoint?.port || 8443;
    const nativePort = nativeEndpoint?.port || 9440;

    // Write to .env
    const envContent = [
      `# Generated on ${new Date().toISOString()}`,
      `CLICKHOUSE_HOST="${host}"`,
      `CLICKHOUSE_USER="default"`,
      `CLICKHOUSE_PASSWORD="${password}"`,
      `CLICKHOUSE_DATABASE="default"`,
      `CLICKHOUSE_HTTPS_PORT="${httpsPort}"`,
      `CLICKHOUSE_NATIVE_PORT="${nativePort}"`,
      `CLICKHOUSE_SERVICE_ID="${serviceId}"`,
      `# Service URL: https://clickhouse.cloud/services/${serviceId}`,
      "",
    ].join("\n");

    fs.writeFileSync(envPath, envContent);
    log.success("Configured .env");

    return {
      host,
      user: "default",
      password,
      database: "default",
      serviceId,
    };
  } catch (error) {
    log.error(`Error: ${error.message}`);
    process.exit(1);
  }
}

await createClickHouseService();