import { Pool } from "pg";
import { config } from "../config";

/**
 * Database Module for Market Data Storage
 * 
 * This module implements the database requirements from the hackathon:
 * - Uses TimescaleDB for efficient time-series storage
 * - Implements batch processing for high-frequency data
 * - Normalizes data using topics and ltp_data tables
 * 
 * Helper Types and Variables:
 * 
 * BatchItem - Interface for market data points:
 *   - topic: MQTT topic (e.g., "index/NIFTY" or "NSE_FO|tokenNumber")
 *   - ltp: Last traded price
 *   - indexName: Index name from INDICES (NIFTY, BANKNIFTY, etc.)
 *   - type: Instrument type (CE for Call, PE for Put)
 *   - strike: Strike price (multiple of index strike difference)
 * 
 * Runtime State Variables:
 *   - pool: PostgreSQL connection pool (initialized in createPool)
 *   - dataBatch: Temporary batch storage (flushed every batchInterval or when full)
 *   - batchTimer: Triggers flush after batchInterval milliseconds
 *     (configured in config.app.batchInterval, defaults to 5000ms)
 *   - topicCache: Maps topic names to DB IDs (loaded in initialize)
 * 
 * Configuration (from config.ts):
 *   - Database: host, port, user, password, database
 *   - Batch: batchSize (default 100), batchInterval (default 5000ms)
 */

// Define a type for batch items
export interface BatchItem {
  topic: string;
  ltp: number;
  indexName?: string;
  type?: string;
  strike?: number;
}

// Initialize database connection pool
let pool: Pool;
let dataBatch: BatchItem[] = [];
let batchTimer: NodeJS.Timeout | null = null;

// Cache topic IDs to avoid repeated lookups
const topicCache = new Map<string, number>();

/**
 * Creates and returns a PostgreSQL connection pool using configuration from config.ts
 * This pool is used for all database operations
 */
export function createPool(): Pool {
  return new Pool({
    host: config.db.host,
    port: config.db.port,
    user: config.db.user,
    password: config.db.password,
    database: config.db.database,
  });
}

/**
 * Initializes the database module and preloads topic cache
 * This improves performance by reducing database lookups for topic IDs
 */
export async function initialize(dbPool: Pool) {
  pool = dbPool;

  try {
    // Preload topic cache from database to optimize lookups
    // This reduces database queries by keeping topic_id values in memory
    const result = await pool.query('SELECT topic_id, topic_name FROM topics');
    for (const row of result.rows) {
      topicCache.set(row.topic_name, row.topic_id);
    }
    console.log(`Database initialized. Loaded ${result.rows.length} topics into cache.`);
  } catch (error) {
    console.error('Error initializing database:', error);
    throw error;
  }
}

/**
 * Gets or creates a topic ID for market data storage
 * 
 * Function flow:
 * 1. Check in-memory cache for topic ID
 * 2. If not in cache, query database
 * 3. If not in database, create new topic
 * 4. Update cache and return ID
 * 
 * @param topicName - MQTT topic name from incoming market data
 * @param indexName - Index name (NIFTY, BANKNIFTY etc.) from config.ts INDICES
 * @param type - Type of instrument
 * @param strike - Strike price for options
 * @returns topic_id from the topics table
 */
export async function getTopicId(
  topicName: string,
  indexName?: string,
  type?: string,
  strike?: number
): Promise<number> {
  // Check in-memory cache first to avoid database lookups
  const cacheKey = topicName;
  if (topicCache.has(cacheKey)) {
    return topicCache.get(cacheKey)!;
  }

  try {
    // Check if topic exists in database
    const result = await pool.query(
      'SELECT topic_id FROM topics WHERE topic_name = $1',
      [topicName]
    );

    if (result.rows.length > 0) {
      const topicId = result.rows[0].topic_id;
      topicCache.set(cacheKey, topicId);
      return topicId;
    }

    // Topic doesn't exist, insert it
    const insertResult = await pool.query(
      'INSERT INTO topics (topic_name, index_name, type, strike) VALUES ($1, $2, $3, $4) RETURNING topic_id',
      [topicName, indexName, type, strike]
    );

    const newTopicId = insertResult.rows[0].topic_id;
    topicCache.set(cacheKey, newTopicId);
    return newTopicId;
  } catch (error) {
    console.error('Error in getTopicId:', error);
    throw error;
  }
}

/**
 * Saves market data using a batching mechanism for better performance
 * 
 * Function flow:
 * 1. Add data to memory batch
 * 2. Start batch timer if not running
 * 3. Force flush if batch size limit reached
 * 
 * Benefits:
 * - Reduces database load by batching inserts
 * - Handles high-frequency data efficiently
 * - Auto-flushes based on size or time
 * 
 * @param topic - MQTT topic from incoming message
 * @param ltp - Last traded price from market data
 * @param indexName - Index name from config.ts INDICES
 * @param type - Type of instrument
 * @param strike - Strike price for options
 */
export function saveToDatabase(
  topic: string,
  ltp: number,
  indexName?: string,
  type?: string,
  strike?: number
) {
  // Add item to batch (uses BatchItem interface defined at top)
  dataBatch.push({ topic, ltp, indexName, type, strike });

  // Start batch timer if not running
  // batchTimer lifecycle:
  // 1. Initialized as null at module level
  // 2. Created here when first item is added to batch
  // 3. Cleared in flushBatch() after writing data
  // 4. Uses config.app.batchInterval (default 5000ms) for timeout
  if (!batchTimer) {
    batchTimer = setTimeout(async () => {
      await flushBatch();
    }, config.app.batchInterval);
  }

  // Flush if batch size threshold reached
  // Uses config.app.batchSize from src/config/index.ts (default 100)
  if (dataBatch.length >= config.app.batchSize) {
    void flushBatch();
  }
}

/**
 * Flushes batched market data to TimescaleDB
 * 
 * Function flow:
 * 1. Clear batch timer and get batch
 * 2. Start database transaction
 * 3. Get topic IDs in parallel
 * 4. Bulk insert all records
 * 5. Commit or rollback transaction
 * 
 * Benefits:
 * - Atomic batch operations
 * - Efficient bulk inserts
 * - Transaction safety
 * - Parallel topic ID resolution
 */
export async function flushBatch() {
  // Clear the batch interval timer (from config.app.batchInterval)
  if (batchTimer) {
    clearTimeout(batchTimer);
    batchTimer = null;
  }

  // Skip if no data to process
  if (dataBatch.length === 0) {
    return;
  }

  // Copy batch for processing and reset main batch
  // This allows new data to accumulate while we process the current batch
  const batchToProcess = [...dataBatch];
  dataBatch = [];

  try {
    // Begin transaction
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      // Process all items and get their topic IDs
      const topicPromises = batchToProcess.map(item =>
        getTopicId(item.topic, item.indexName, item.type, item.strike)
      );
      const topicIds = await Promise.all(topicPromises);

      // Prepare the values for bulk insert
      const values = batchToProcess.map((item, index) => 
        `(${topicIds[index]}, ${item.ltp}, NOW())`
      ).join(',');

      // Perform bulk insert
      await client.query(`
        INSERT INTO ltp_data (topic_id, ltp, received_at)
        VALUES ${values}
      `);

      await client.query('COMMIT');
      console.log(`Successfully inserted ${batchToProcess.length} records`);
    } catch (error) {
      await client.query('ROLLBACK');
      console.error('Error in flushBatch transaction:', error);
      throw error;
    } finally {
      client.release();
    }
  } catch (error) {
    console.error('Error in flushBatch:', error);
    throw error;
  }
}

/**
 * Performs cleanup operations when shutting down
 * Ensures all pending data is saved and connections are closed
 */
export async function cleanupDatabase() {
  // Flush any remaining items in the batch
  if (dataBatch.length > 0) {
    await flushBatch();
  }

  // Close the database pool
  if (pool) {
    await pool.end();
  }

  console.log("Database cleanup completed");
}
