# SUI Arbitrage Analyzer V2

An enhanced version of the SUI arbitrage analyzer that monitors multiple trading pairs across different DEXes on the SUI network.

## Supported Pairs

- SUI/USDC (Cetus) - SUI/USDT (Bluefin)
- DEEP/SUI (Both Cetus and Bluefin)
- WBTC/SUI (Both Cetus and Bluefin)

## Features

- Real-time price monitoring
- Liquidity tracking
- Volume analysis
- Arbitrage opportunity detection
- Tick data collection
- Rate limiting and error handling
- Continuous data collection with configurable intervals

## Prerequisites

- Node.js v16 or higher
- PostgreSQL/TimescaleDB database
- TypeScript
- Yarn or npm

## Installation

1. Clone the repository and navigate to the directory:
```bash
cd sui_dashboard_v2
```

2. Install dependencies:
```bash
yarn install
# or
npm install
```

3. Create a `.env` file in the project root with the following variables:
```env
TIMESCALE_HOST=your_db_host
TIMESCALE_PORT=your_db_port
TIMESCALE_DB=your_db_name
TIMESCALE_USER=your_db_user
TIMESCALE_PASSWORD=your_db_password
DB_SSL=false
CRYPTOCOMPARE_API_KEY=your_api_key  # Optional
```

## Running the Analyzer

Start the analyzer in development mode with auto-restart on file changes:
```bash
yarn dev
# or
npm run dev
```

Start the analyzer in production mode:
```bash
yarn start
# or
npm start
```

The analyzer will:
1. Create necessary database tables if they don't exist
2. Start collecting data immediately
3. Continue running and collecting data every 5 minutes
4. Log all activities to both console and a log file

## Database Schema

The analyzer uses four main tables:

1. `sui_price_data_v2`: Stores price and volume data
2. `sui_pool_data_v2`: Stores pool-specific data including liquidity
3. `sui_arbitrage_data_v2`: Stores arbitrage opportunities
4. `sui_ticks_data_v2`: Stores detailed tick data for each pool

## Monitoring

- Check the console output for real-time updates
- Review the `arbitrage_analyzer_v2.log` file for historical data
- Query the database tables for detailed analysis

## Error Handling

The analyzer includes:
- Rate limiting for API calls
- Automatic retries for failed requests
- Graceful error handling
- Detailed error logging

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request 