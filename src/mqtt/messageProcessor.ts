import mqtt from "mqtt";
import * as marketdata from "../proto/market_data_pb";
import * as subscriptionManager from "./subscriptionManager";
import * as db from "../db";
import * as utils from "../utils";
import { config } from "../config";

// Store LTP values for indices
const indexLtpMap = new Map<string, number>();
const atmStrikeMap = new Map<string, number>();

export async function processMessage(
  topic: string,
  message: Buffer,
  client: mqtt.MqttClient
) {
  try {
    // TODO: Implement this function
    // 1. Parse the message (it's likely in JSON format)
    // 2. Extract LTP value
    // 3. If it's an index topic, calculate ATM and subscribe to options (This is one time operation only)
    // 4. Save data to database

    // Decoding logic
    let decoded: any = null;
    let ltpValues: number[] = [];

    // Try decoding as MarketData
    try {
      decoded = marketdata.marketdata.MarketData.decode(
        new Uint8Array(message)
      );
      if (decoded && typeof decoded.ltp === "number") {
        ltpValues.push(decoded.ltp);
      }
    } catch (err) {
      // Try decoding as MarketDataBatch
      try {
        decoded = marketdata.marketdata.MarketDataBatch.decode(
          new Uint8Array(message)
        );
        if (decoded && Array.isArray(decoded.data)) {
          ltpValues = decoded.data
            .map((d: any) => d.ltp)
            .filter((v: any) => typeof v === "number");
        }
      } catch (batchErr) {
        // Try decoding as JSON
        try {
          decoded = JSON.parse(message.toString());
          if (decoded && typeof decoded.ltp === "number") {
            ltpValues.push(decoded.ltp);
          }
        } catch (jsonErr) {
          console.error(
            "Failed to decode message as protobuf or JSON for topic:",
            topic
          );
        }
      }
    }

    // Process each decoded LTP (Last Traded Price) value
    for (const ltp of ltpValues) {
      // Handle index topics (e.g., "index/NIFTY") for ATM calculation
      if (topic.startsWith(`${config.app.indexPrefix}/`)) {
        const indexName = topic.split('/')[1];
        const prevLtp = indexLtpMap.get(indexName);
        
        // Track current LTP in memory for ATM calculations
        indexLtpMap.set(indexName, ltp);
        
        // Calculate ATM strike using index-specific strike difference
        // E.g., NIFTY: 50, BANKNIFTY: 100, etc.
        const atmStrike = utils.getAtmStrike(indexName, ltp);
        
        // Check first message status from subscription manager
        const isFirstMessage = subscriptionManager.isFirstIndexMessage.get(indexName);
        
        // Update option subscriptions when:
        // 1. First message ever for this index, or
        // 2. ATM strike changed by at least one strike difference
        // This ensures we track the right options as the market moves
        if (isFirstMessage || 
            (prevLtp !== undefined && 
             Math.abs(atmStrike - (atmStrikeMap.get(indexName) || 0)) >= utils.getStrikeDiff(indexName))) {
          
          // Update tracked ATM strike
          atmStrikeMap.set(indexName, atmStrike);
          
          // Subscribe to options: ATM ± 5 strikes, both CE and PE
          await subscriptionManager.subscribeToAtmOptions(client, indexName, atmStrike);
          
          // Reset first message flag after initial subscription
          if (isFirstMessage) {
            subscriptionManager.isFirstIndexMessage.set(indexName, false);
          }
        }
      }
      
      // Prepare metadata for database storage
      let indexName: string | undefined;
      let type: string | undefined;
      let strike: number | undefined;
      
      if (topic.startsWith(`${config.app.indexPrefix}/`)) {
        // For index topics, we just need the index name
        indexName = topic.split('/')[1];
      } else if (topic.startsWith('NSE_FO|')) {
        // For option topics, metadata comes from token API responses
        // This is handled during subscription in subscriptionManager
      }
      
      // Store data point using batched database writes
      // This includes topic metadata for efficient querying
      db.saveToDatabase(topic, ltp, indexName, type, strike);
    }
  } catch (error) {
    console.error("Error processing message:", error);
  }
}
