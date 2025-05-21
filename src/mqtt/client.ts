import mqtt from "mqtt";
import { config } from "../config";


// Market data connection state
let reconnectCount = 0;                    // Tracks failed connection attempts for backoff
const MAX_RECONNECT_DELAY = 5000;          // Cap retry delay at 5s for quick recovery
const HEALTH_CHECK_INTERVAL = 5000;        // Check connection health every 5s
let healthCheckTimer: NodeJS.Timeout | null = null;  // Timer for stale connection checks
let lastPacketTime = Date.now();           // Tracks last successful data transmission

export function createClient(): mqtt.MqttClient {
  // Use TLS for secure market data transmission (port 8883)
  // Critical for protecting sensitive financial data
  const connectUrl = `mqtts://${config.mqtt.host}:${config.mqtt.port}`;

  // Critical market data connection settings
  const options: mqtt.IClientOptions = {
    // Connection settings
    clientId: config.mqtt.clientId,
    clean: false,                            // Maintain session on reconnect
    connectTimeout: 2000,                    // Fast timeout detection
    username: config.mqtt.username,
    password: config.mqtt.password,
    
    // Reliability settings for market data
    reconnectPeriod: 100,                    // Aggressive initial reconnect (100ms)
    keepalive: 5,                            // Frequent keepalive checks
    queueQoSZero: false,                     // Don't queue non-guaranteed messages
    
    // TLS settings
    rejectUnauthorized: false,               // Allow self-signed certs
    protocol: 'mqtts',                       // Secure connection
    
    // Optimization
    properties: {
      maximumPacketSize: 1048576,            // 1MB packet size for large batches
      requestResponseInformation: true,
      requestProblemInformation: true
    }
  };

  const client = mqtt.connect(connectUrl, options);

  // Set up internal connection monitoring
  client.on('connect', () => {
    reconnectCount = 0;
    startHealthCheck(client);
  });

  // Handle reconnection with exponential backoff for reliability
  client.on('reconnect', () => {
    reconnectCount++;
    const delay = Math.min(100 * Math.pow(1.5, reconnectCount), MAX_RECONNECT_DELAY);
    client.options.reconnectPeriod = delay;
  });

  // Clean up health check on connection close
  client.on('close', () => {
    if (healthCheckTimer) {
      clearInterval(healthCheckTimer);
      healthCheckTimer = null;
    }
  });

  // Monitor packet flow for connection health
  client.on('packetsend', () => {
    lastPacketTime = Date.now();
  });

  client.on('packetreceive', () => {
    lastPacketTime = Date.now();
  });

  return client;
}

/**
 * Active Connection Health Monitoring
 * 
 * Ensures market data connection remains active by:
 * 1. Tracking time since last packet
 * 2. Forcing reconnection if no data for 10s
 * 3. Running checks every 5s
 * 
 * This aggressive monitoring is critical for:
 * - Minimizing data loss
 * - Quick recovery from network issues
 * - Maintaining real-time market data flow
 */
function startHealthCheck(client: mqtt.MqttClient) {
  if (healthCheckTimer) {
    clearInterval(healthCheckTimer);
  }

  healthCheckTimer = setInterval(() => {
    const timeSinceLastPacket = Date.now() - lastPacketTime;
    
    // If no packets for 10 seconds, force reconnection
    if (timeSinceLastPacket > 10000) {
      console.error('Market data connection stale - forcing reconnection');
      client.end(true, {}, () => {
        client.reconnect();
      });
    }
  }, HEALTH_CHECK_INTERVAL);
}
