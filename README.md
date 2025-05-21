# TRADO <> IIT Ropar Hackathon

Welcome to the Trado <> IIT Ropar Hackathon! This is the first part of a two part hackathon conducted by Trado, at IIT Ropar.

This project involves building a service that connects to an MQTT broker to subscribe to market data, process it, and store it in a TimescaleDB database.

## Task Overview

You need to:

1. Connect to an EMQX MQTT broker
2. Subscribe to index data (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, etc.)
3. Process incoming market data
4. Calculate ATM (At-The-Money) strike prices
5. Subscribe to options around those ATM strikes
6. Store all data in a PostgreSQL/TimescaleDB database

## Setup Instructions

### Prerequisites

- Node.js (v16+ recommended)
- PostgreSQL with TimescaleDB extension
- npm or yarn

### Database Setup

1. Install PostgreSQL and TimescaleDB extension
2. Create a new database: 
   ```
   createdb market_data
   ```
3. Run the schema script:
   ```
   psql -d market_data -f scripts/db-schema.sql
   ```

### Project Setup

1. Clone this repository
2. Install dependencies:
   ```
   npm install
   ```
3. Copy `.env.example` to `.env` and update the values
4. Start the application:
   ```
   npm start
   ```

## Implementation Requirements

### 1. MQTT Connection

- Connect to the MQTT broker using the provided credentials
- The connection should be robust with reconnection capability

### 2. Index Subscription

- Subscribe to the following indices:
  - NIFTY
  - BANKNIFTY
  - FINNIFTY
  - MIDCPNIFTY
- The topic format is `{INDEX_PREFIX}/{indexName}` (e.g., `index/NIFTY`)

### 3. ATM Strike Calculation

- When the first message for an index is received, calculate its ATM strike
- The ATM strike is the multiple of the strike difference that is closest to the LTP
- Strike differences:
  - NIFTY: 50
  - BANKNIFTY: 100
  - FINNIFTY: 50
  - MIDCPNIFTY: 25
  - BANKEX: 100
  - SENSEX: 100

### 4. Options Subscription

- For each index, subscribe to options at ATM and ATM ± 5 strikes
- For each strike, subscribe to both CE (Call) and PE (Put) options
- Use the API to get token numbers:
  - `https://api.trado.trade/token?index=NIFTY&expiryDate=22-05-2025&optionType=ce&strikePrice=25000`
- The subscription format is `NSE_FO|{tokenNumber}`
- Expiry dates:
  - NIFTY: 22-05-2025
  - BANKNIFTY: 29-05-2025
  - FINNIFTY: 29-05-2025
  - MIDCPNIFTY: 29-05-2025

### 5. Database Storage

- Store all received data in the database
- Implement batch processing for efficiency
- Normalize data by storing topic information separately

## Tips for Success

1. **Async/Await**: Use async/await for all asynchronous operations
2. **Batch Processing**: Implement batching for database writes
3. **Error Handling**: Add proper error handling throughout
4. **Logging**: Add meaningful logs for debugging
5. **Code Structure**: Keep your code modular and well-organized

Good luck! 
