/**
 * MQTT Subscription Manager
 * 
 * This module manages MQTT topic subscriptions for market data:
 * 1. Initial index subscriptions (NIFTY, BANKNIFTY, etc.)
 * 2. Option chain subscriptions based on ATM strikes
 * 3. Token fetching for option topics
 * 
 * Key Features:
 * - Avoids duplicate subscriptions
 * - Maintains ATM strike tracking
 * - Fetches option tokens from API
 * - Manages CE/PE subscriptions
 */

import mqtt from "mqtt";
import { config, INDICES, EXPIRY_DATES, STRIKE_RANGE } from "../config";
import * as utils from "../utils";

// Runtime state management
/** Tracks active topic subscriptions to prevent duplicates */
export const activeSubscriptions = new Set<string>();

/** Tracks first message flag for each index to handle initial ATM subscriptions */
export const isFirstIndexMessage = new Map<string, boolean>();

/**
 * Subscribes to all configured market indices
 * Indices are defined in config.ts (NIFTY, BANKNIFTY, etc.)
 * Topics follow format: {indexPrefix}/{indexName}
 */
export function subscribeToAllIndices(client: mqtt.MqttClient) {
  INDICES.forEach((indexName) => {
    const topic = `index/${indexName}`;
    console.log(`Subscribing to index: ${topic}`);
    client.subscribe(topic);
    activeSubscriptions.add(topic);
  });
}

/**
 * Initializes tracking state for index messages
 * Sets first message flag to true for all indices
 * Used to trigger initial ATM option subscriptions
 */
export function initializeFirstMessageTracking() {
  INDICES.forEach((indexName) => {
    isFirstIndexMessage.set(indexName, true);
  });
}

/**
 * Subscribes to options around an ATM strike price
 * 
 * Process:
 * 1. Calculates strike prices (ATM ± STRIKE_RANGE)
 * 2. Fetches tokens for both CE and PE options
 * 3. Subscribes to available option topics
 * 4. Tracks subscriptions to prevent duplicates
 * 
 * @param client - MQTT client to use for subscriptions
 * @param indexName - Index to subscribe options for (e.g., "NIFTY")
 * @param atmStrike - Current ATM strike price
 */
export async function subscribeToAtmOptions(
  client: mqtt.MqttClient,
  indexName: string,
  atmStrike: number
) {
  console.log(`Subscribing to ${indexName} options around ATM ${atmStrike}`);

  const strikeDiff = utils.getStrikeDiff(indexName);
  const strikes = [];

  // Calculate strikes around ATM
  for (let i = -STRIKE_RANGE; i <= STRIKE_RANGE; i++) {
    strikes.push(atmStrike + i * strikeDiff);
  }

  // Subscribe to CE and PE options for each strike
  for (const strike of strikes) {
    // Get tokens for both CE and PE
    const ceToken = await getOptionToken(indexName, strike, "ce");
    const peToken = await getOptionToken(indexName, strike, "pe");

    // Subscribe to CE option if token available
    if (ceToken) {
      const ceTopic = `index/NSE_FO|${ceToken}`;  
      if (!activeSubscriptions.has(ceTopic)) {
        console.log(`Subscribing to CE option: ${ceTopic}`);
        client.subscribe(ceTopic);
        activeSubscriptions.add(ceTopic);
      }
    }

    // Subscribe to PE option if token available  
    if (peToken) {
      const peTopic = `index/NSE_FO|${peToken}`;
      if (!activeSubscriptions.has(peTopic)) {
        console.log(`Subscribing to PE option: ${peTopic}`);
        client.subscribe(peTopic);
        activeSubscriptions.add(peTopic);
      }
    }
  }
}

/**
 * Fetches option token from Trado API
 * 
 * Uses configured expiry dates from config.ts
 * API endpoint: https://api.trado.trade/token
 * 
 * @param indexName - Index name (e.g., "NIFTY")
 * @param strikePrice - Option strike price
 * @param optionType - "ce" for Call or "pe" for Put
 * @returns Token string or null if fetch fails
 */
export async function getOptionToken(
  indexName: string,
  strikePrice: number,
  optionType: "ce" | "pe"
): Promise<string | null> {
  try {
    const expiryDate = EXPIRY_DATES[indexName as keyof typeof EXPIRY_DATES];
    const url = `https://api.trado.trade/token?index=${indexName}&expiryDate=${expiryDate}&optionType=${optionType}&strikePrice=${strikePrice}`;

    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    return data.token || null;

  } catch (error) {
    console.error(
      `Error fetching token for ${indexName} ${strikePrice} ${optionType}:`,
      error
    );
    return null;
  }
}
