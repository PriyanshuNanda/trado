// Function to get the strike difference for an index
export function getStrikeDiff(indexName: string): number {
  switch (indexName.toLowerCase()) {
    case "nifty":
      return 50;
    case "banknifty":
      return 100;
    case "finnifty":
      return 50;
    case "midcpnifty":
      return 25;
    case "bankex":
      return 100;
    case "sensex":
      return 100;
    default:
      return 100; // Default
  }
}

// Calculate ATM strike based on index LTP
export function getAtmStrike(indexName: string, ltp: number): number {
  const strikeDiff = getStrikeDiff(indexName);
  return Math.round(ltp / strikeDiff) * strikeDiff;
}

// Format option topic for subscription
export function getOptionTopic(indexName: string, tokenNumber: string): string {
  return `NSE_FO|${tokenNumber}`;
}
