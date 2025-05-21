import * as dotenv from "dotenv";
dotenv.config();

import { config } from "./config";
import * as db from "./db";
import * as mqttClient from "./mqtt/client";
import * as messageProcessor from "./mqtt/messageProcessor";
import * as subscriptionManager from "./mqtt/subscriptionManager";

async function start() {
  try {
    // Initialize the database
    const pool = db.createPool();
    db.initialize(pool);

    // Initialize subscription manager
    subscriptionManager.initializeFirstMessageTracking();

    // Create MQTT client
    const client = mqttClient.createClient();

    // MQTT event handlers
    client.on("connect", () => {
      console.log("Connected to MQTT broker");

      // Subscribe to all index topics
      subscriptionManager.subscribeToAllIndices(client);
    });

    client.on("message", (topic: string, message: Buffer) => {
      messageProcessor.processMessage(topic, message, client);
    });

    client.on("error", (err: Error) => {
      console.error("MQTT connection error:", err);
    });

    client.on("reconnect", () => {
      console.log("Attempting to reconnect to MQTT broker");
    });

    client.on("close", () => {
      console.log("MQTT connection closed");
    });

    // Handle graceful shutdown
    process.on("SIGINT", async () => {
      await db.cleanupDatabase();
      client.end();
      process.exit();
    });
  } catch (error) {
    console.error("Application error:", error);
    process.exit(1);
  }
}

start();
