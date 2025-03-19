import { initCetusSDK } from '@cetusprotocol/cetus-sui-clmm-sdk'
import bluefinExchange from '@api/bluefin-exchange';
import { TickMath } from '@cetusprotocol/cetus-sui-clmm-sdk'
import BN from 'bn.js'
import ccxt, { binance } from 'ccxt'
import * as pg from 'pg'
import dotenv from 'dotenv'
import fs from 'fs'
import path from 'path'
import axios from 'axios'

// Load environment variables
dotenv.config({ path: path.resolve(__dirname, '.env') })

// Database connection details
const DB_HOST = process.env.TIMESCALE_HOST
const DB_PORT = process.env.TIMESCALE_PORT
const DB_NAME = process.env.TIMESCALE_DB
const DB_USER = process.env.TIMESCALE_USER
const DB_PASSWORD = process.env.TIMESCALE_PASSWORD
const DB_SSL = process.env.DB_SSL || 'false'

// Pool addresses for SUI pairs
const POOL_ADDRESSES = {
  CETUS: {
    SUI_USDC: '0xb8d7d9e66a60c239e7a60110efcf8de6c705580ed924d0dde141f4a0e2c90105',
    DEEP_SUI: '0xe01243f37f712ef87e556afb9b1d03d0fae13f96d324ec912daffc339dfdcbd2',
    WBTC_SUI: '0xe0c526aa27d1729931d0051a318d795ad0299998898e4287d9da1bf095b49658'
  },
  BLUEFIN: {
    SUI_USDT: '0x3b585786b13af1d8ea067ab37101b6513a05d2f90cfe60e8b1d9e1b46a63c4fa',
    DEEP_SUI: '0x1b06371d74082856a1be71760cf49f6a377d050eb57afd017f203e89b09c89a2',
    WBTC_SUI: '0xd7d53e235c8a1db5e30bbde563053490db9b876ec8752b9053fee33ed845843b'
  }
}

// Token decimals
const TOKEN_DECIMALS: { [key: string]: number } = {
  SUI: 9,
  USDC: 6,
  USDT: 6,
  DEEP: 9,
  WBTC: 8
}

// API endpoints for fallbacks
const HYBLOCK_API_BASE_URL = 'https://api1.hyblockcapital.com/v1'
const DEFILLAMA_API_BASE_URL = 'https://coins-api.llama.fi/prices/current'
const CRYPTOCOMPARE_API_URL = 'https://min-api.cryptocompare.com/data/price'

// Add CryptoCompare API configuration
const CRYPTOCOMPARE_API_KEY = process.env.CRYPTOCOMPARE_API_KEY || ''

// Initialize SDK
const cetusClmmSDK = initCetusSDK({ network: 'mainnet' })

// Create a database connection pool
const pool = new pg.Pool({
  host: DB_HOST,
  port: parseInt(DB_PORT || '34569'),
  database: DB_NAME,
  user: DB_USER,
  password: DB_PASSWORD,
  ssl: DB_SSL === 'true' ? { rejectUnauthorized: false } : false
})

// Add rate limiting helper
const rateLimiter = {
  lastCallTime: 0,
  minInterval: 5000, // 5 seconds between requests
  async wait() {
    const now = Date.now();
    const timeSinceLastCall = now - this.lastCallTime;
    if (timeSinceLastCall < this.minInterval) {
      await new Promise(resolve => setTimeout(resolve, this.minInterval - timeSinceLastCall));
    }
    this.lastCallTime = Date.now();
  }
}

// Add a timestamp tracker for the last successful data collection
let lastSuccessfulCollection = 0;
const MIN_COLLECTION_INTERVAL = 10000; // 10 seconds minimum between collections

// Enhanced logging with severity levels
enum LogLevel {
  INFO = 'INFO',
  WARN = 'WARN',
  ERROR = 'ERROR',
  DEBUG = 'DEBUG'
}

function log(message: string, level: LogLevel = LogLevel.INFO, error?: Error) {
  const timestamp = new Date().toISOString()
  const logMessage = `[${timestamp}] [${level}] ${message}${error ? `\nError: ${error.message}\nStack: ${error.stack}` : ''}`
  
  console.log(logMessage)
  fs.appendFileSync(path.resolve(__dirname, 'arbitrage_analyzer_v2.log'), `${logMessage}\n`)
  
  // Additional error reporting for critical issues
  if (level === LogLevel.ERROR) {
    // Here you could add integration with error reporting services
    // like Sentry, DataDog, etc.
  }
}

// Add process error handlers
process.on('uncaughtException', (error) => {
  log('Uncaught Exception:', LogLevel.ERROR, error)
  // Give time for logs to be written
  setTimeout(() => process.exit(1), 1000)
})

process.on('unhandledRejection', (reason, promise) => {
  log(`Unhandled Rejection at: ${promise}\nReason: ${reason}`, LogLevel.ERROR)
})

// Add graceful shutdown handler
async function gracefulShutdown(signal: string) {
  log(`Received ${signal}. Starting graceful shutdown...`, LogLevel.INFO)
  
  try {
    // Close database pool
    await pool.end()
    log('Database pool closed successfully', LogLevel.INFO)
    
    // Any other cleanup tasks can go here
    
    log('Graceful shutdown completed', LogLevel.INFO)
    process.exit(0)
  } catch (error) {
    log('Error during graceful shutdown:', LogLevel.ERROR, error as Error)
    process.exit(1)
  }
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'))
process.on('SIGINT', () => gracefulShutdown('SIGINT'))

// Helper function to check if an error is related to rate limiting
function isRateLimitError(error: any): boolean {
  if (!error) return false
  
  const errorMessage = error.toString().toLowerCase()
  return (
    errorMessage.includes('rate limit') ||
    errorMessage.includes('ratelimit') ||
    errorMessage.includes('too many requests') ||
    errorMessage.includes('429') ||
    errorMessage.includes('ddos protection') ||
    (error.name && error.name === 'DDoSProtection') ||
    (error.status && error.status === 429)
  )
}

// Define interfaces for different types of data
interface PriceData {
  timestamp: Date;
  source: string;
  pair: string;
  price: number;
  volume24h: number;
  data_source?: string;
}

interface PoolData {
  timestamp: Date;
  source: string;
  pair: string;
  pool_address: string;
  liquidity: number;
  liquidity_usd: number;
  volume1h: number;
  volume24h?: number;
  fee_rate?: number;
  ticks_data?: any[];
  raw_pool_data?: any;
}

// Function to get token decimals safely
function getTokenDecimals(token: string): number {
  const decimals = TOKEN_DECIMALS[token];
  if (decimals === undefined) {
    log(`Warning: No decimals defined for token ${token}, using default of 9`);
    return 9;
  }
  return decimals;
}

// Function to ensure all necessary database tables exist
async function ensureTablesExist() {
  const client = await pool.connect()
  try {
    // Create enhanced sui_price_data table
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.sui_price_data_v2 (
        timestamp TIMESTAMPTZ NOT NULL,
        source TEXT NOT NULL,
        pair TEXT NOT NULL,
        price NUMERIC NOT NULL,
        volume_24h NUMERIC NOT NULL,
        data_source VARCHAR(50),
        PRIMARY KEY (timestamp, source, pair)
      );
    `)

    // Create enhanced sui_pool_data table
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.sui_pool_data_v2 (
        timestamp TIMESTAMPTZ NOT NULL,
        source TEXT NOT NULL,
        pair TEXT NOT NULL,
        pool_address TEXT NOT NULL,
        liquidity NUMERIC NOT NULL,
        liquidity_usd NUMERIC NOT NULL,
        volume_1h NUMERIC NOT NULL,
        volume_24h NUMERIC,
        fee_rate NUMERIC,
        raw_data JSONB,
        PRIMARY KEY (timestamp, pool_address)
      );
    `)

    // Create enhanced sui_arbitrage_data table
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.sui_arbitrage_data_v2 (
        timestamp TIMESTAMPTZ NOT NULL,
        pair TEXT NOT NULL,
        source1 TEXT NOT NULL,
        source2 TEXT NOT NULL,
        price1 NUMERIC NOT NULL,
        price2 NUMERIC NOT NULL,
        price_delta NUMERIC NOT NULL,
        PRIMARY KEY (timestamp, pair, source1, source2)
      );
    `)

    // Create enhanced sui_ticks_data table
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.sui_ticks_data_v2 (
        timestamp TIMESTAMPTZ NOT NULL,
        pool_address TEXT NOT NULL,
        pair TEXT NOT NULL,
        tick_index INTEGER NOT NULL,
        liquidity_net NUMERIC,
        liquidity_gross NUMERIC,
        sqrt_price NUMERIC,
        fee_growth_outside_a NUMERIC,
        fee_growth_outside_b NUMERIC,
        seconds_outside NUMERIC,
        source TEXT NOT NULL,
        object_id TEXT,
        rewarders_growth_outside JSONB,
        tick_data JSONB,
        PRIMARY KEY (timestamp, pool_address, tick_index)
      );
    `)

    // Create indices for better query performance
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_sui_price_data_v2_timestamp ON public.sui_price_data_v2 (timestamp);
      CREATE INDEX IF NOT EXISTS idx_sui_pool_data_v2_timestamp ON public.sui_pool_data_v2 (timestamp);
      CREATE INDEX IF NOT EXISTS idx_sui_arbitrage_data_v2_timestamp ON public.sui_arbitrage_data_v2 (timestamp);
      CREATE INDEX IF NOT EXISTS idx_sui_ticks_data_v2_timestamp ON public.sui_ticks_data_v2 (timestamp);
    `)

    log('Successfully created all required tables')
  } catch (error) {
    log(`Error creating tables: ${error}`)
    throw error
  } finally {
    client.release()
  }
}

// Function to save price data
async function savePriceData(data: PriceData) {
  if (!data) return
  
  const client = await pool.connect()
  try {
    const query = `
      INSERT INTO public.sui_price_data_v2 (timestamp, source, pair, price, volume_24h, data_source)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (timestamp, source, pair) 
      DO UPDATE SET price = $4, volume_24h = $5, data_source = $6
    `
    
    await client.query(query, [
      data.timestamp,
      data.source,
      data.pair,
      data.price,
      data.volume24h,
      data.data_source
    ])
    
    log(`Saved price data for ${data.source} ${data.pair}: ${data.price}`)
  } catch (error) {
    log(`Error saving price data: ${error}`)
  } finally {
    client.release()
  }
}

// Function to save pool data
async function savePoolData(data: PoolData) {
  if (!data) return
  
  const client = await pool.connect()
  try {
    const query = `
      INSERT INTO public.sui_pool_data_v2 (
        timestamp, source, pair, pool_address, liquidity, liquidity_usd,
        volume_1h, volume_24h, fee_rate, raw_data
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      ON CONFLICT (timestamp, pool_address) 
      DO UPDATE SET 
        liquidity = $5,
        liquidity_usd = $6,
        volume_1h = $7,
        volume_24h = $8,
        fee_rate = $9,
        raw_data = $10
    `
    
    await client.query(query, [
      data.timestamp,
      data.source,
      data.pair,
      data.pool_address,
      data.liquidity,
      data.liquidity_usd,
      data.volume1h,
      data.volume24h || 0,
      data.fee_rate || 0,
      JSON.stringify({
        ticks_data: data.ticks_data,
        raw_pool_data: data.raw_pool_data
      })
    ])
    
    log(`Saved pool data for ${data.source} ${data.pair}`)
  } catch (error) {
    log(`Error saving pool data: ${error}`)
  } finally {
    client.release()
  }
}

// Function to save arbitrage data
async function saveArbitrageData(
  timestamp: Date,
  pair: string,
  source1: string,
  source2: string,
  price1: number,
  price2: number
) {
  const client = await pool.connect()
  try {
    const priceDelta = ((price2 - price1) / price1) * 100
    
    const query = `
      INSERT INTO public.sui_arbitrage_data_v2 (
        timestamp, pair, source1, source2, price1, price2, price_delta
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (timestamp, pair, source1, source2) 
      DO UPDATE SET 
        price1 = $5,
        price2 = $6,
        price_delta = $7
    `
    
    await client.query(query, [
      timestamp,
      pair,
      source1,
      source2,
      price1,
      price2,
      priceDelta
    ])
    
    log(`Saved arbitrage data for ${pair} between ${source1}-${source2}: ${priceDelta.toFixed(2)}%`)
  } catch (error) {
    log(`Error saving arbitrage data: ${error}`)
  } finally {
    client.release()
  }
}

// Function to save ticks data
async function saveTicksData(
  timestamp: Date,
  poolAddress: string,
  pair: string,
  source: string,
  ticksData: any[]
) {
  if (!ticksData || !ticksData.length) return
  
  const client = await pool.connect()
  try {
    for (const tick of ticksData) {
      const query = `
        INSERT INTO public.sui_ticks_data_v2 (
          timestamp, pool_address, pair, tick_index, liquidity_net, liquidity_gross,
          sqrt_price, fee_growth_outside_a, fee_growth_outside_b, seconds_outside,
          source, object_id, rewarders_growth_outside, tick_data
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (timestamp, pool_address, tick_index) 
        DO UPDATE SET
          liquidity_net = $5,
          liquidity_gross = $6,
          sqrt_price = $7,
          fee_growth_outside_a = $8,
          fee_growth_outside_b = $9,
          seconds_outside = $10,
          rewarders_growth_outside = $13,
          tick_data = $14
      `
      
      await client.query(query, [
        timestamp,
        poolAddress,
        pair,
        tick.index || tick.tickIdx || 0,
        tick.liquidityNet?.toString() || '0',
        tick.liquidityGross?.toString() || '0',
        tick.sqrtPrice?.toString() || '0',
        tick.feeGrowthOutsideA?.toString() || '0',
        tick.feeGrowthOutsideB?.toString() || '0',
        tick.secondsOutside?.toString() || '0',
        source,
        tick.objectId || '',
        JSON.stringify(tick.rewardersGrowthOutside || []),
        JSON.stringify(tick)
      ])
    }
    
    log(`Saved ${ticksData.length} ticks for ${source} ${pair} pool ${poolAddress}`)
  } catch (error) {
    log(`Error saving ticks data: ${error}`)
  } finally {
    client.release()
  }
}

// Add retry mechanism for API calls
async function withRetry<T>(
  operation: () => Promise<T>,
  name: string,
  maxRetries = 3,
  baseDelay = 1000
): Promise<T> {
  let lastError: Error | null = null;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      await rateLimiter.wait();
      return await operation();
    } catch (error) {
      lastError = error as Error;
      if (attempt < maxRetries) {
        const delay = baseDelay * Math.pow(2, attempt - 1);
        log(`${name} attempt ${attempt} failed. Retrying in ${delay}ms...`, LogLevel.WARN, error as Error);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }
  
  throw new Error(`${name} failed after ${maxRetries} attempts. Last error: ${lastError?.message}`);
}

// Function to get Cetus pool data
async function getCetusPoolData(poolAddress: string, pair: string): Promise<PoolData | null> {
  try {
    const pool = await cetusClmmSDK.Pool.getPool(poolAddress)
    const ticksData = await cetusClmmSDK.Pool.fetchTicks({
      pool_id: poolAddress,
      coinTypeA: pool.coinTypeA,
      coinTypeB: pool.coinTypeB,
    })
    
    // Calculate prices and liquidity based on pool data
    const timestamp = new Date()
    const [token0, token1] = pair.split('/')
    const decimalsA = getTokenDecimals(token0)
    const decimalsB = getTokenDecimals(token1)
    
    const sqrtPriceX64 = new BN(pool.current_sqrt_price)
    const price = TickMath.sqrtPriceX64ToPrice(sqrtPriceX64, decimalsB, decimalsA)
    const priceNumber = 1 / parseFloat(price.toString())
    
    // Calculate liquidity
    const liquidity = Number(pool.liquidity) / 1e18
    const liquidityUsd = liquidity * priceNumber
    
    // Calculate volume from fee growth
    const feeRate = Number(pool.fee_rate) / 10000
    const feeGrowthA = Number(pool.fee_growth_global_a) / Math.pow(10, decimalsA)
    const feeGrowthB = Number(pool.fee_growth_global_b) / Math.pow(10, decimalsB)
    const volume1h = (feeGrowthA + feeGrowthB * priceNumber) / feeRate
    
    log(`Cetus ${pair} price: ${priceNumber}, liquidity: ${liquidityUsd}`)
    
    // Save ticks data
    await saveTicksData(timestamp, poolAddress, pair, 'cetus', ticksData)
    
    return {
      timestamp,
      source: 'cetus',
      pair,
      pool_address: poolAddress,
      liquidity,
      liquidity_usd: liquidityUsd,
      volume1h,
      fee_rate: feeRate,
      ticks_data: ticksData,
      raw_pool_data: pool
    }
  } catch (error) {
    log(`Error fetching Cetus pool data for ${pair}: ${error}`)
    return null
  }
}

// Function to get Bluefin pool data
async function getBluefinPoolData(poolAddress: string, pair: string): Promise<PoolData | null> {
  try {
    const response = await bluefinExchange.getPoolsInfo({ pools: poolAddress })
    
    if (response?.data?.[0]) {
      const poolData = response.data[0]
      const timestamp = new Date()
      
      const liquidityUsd = parseFloat(poolData.tvl || '0')
      const volume24h = poolData.day?.volume ? parseFloat(poolData.day.volume) : 0
      const volume1h = volume24h / 24
      
      // Fetch and save ticks data
      const ticksData = await getBluefinTicks(poolAddress)
      if (ticksData.length > 0) {
        await saveTicksData(timestamp, poolAddress, pair, 'bluefin', ticksData)
      }
      
      log(`Bluefin ${pair} liquidity: $${liquidityUsd}, 24h volume: $${volume24h}`)
      
      return {
        timestamp,
        source: 'bluefin',
        pair,
        pool_address: poolAddress,
        liquidity: 0, // Bluefin doesn't provide raw liquidity
        liquidity_usd: liquidityUsd,
        volume1h,
        volume24h,
        ticks_data: ticksData,
        raw_pool_data: poolData
      }
    }
    
    return null
  } catch (error) {
    log(`Error fetching Bluefin pool data for ${pair}: ${error}`)
    return null
  }
}

// Function to get Bluefin ticks
async function getBluefinTicks(poolAddress: string): Promise<any[]> {
  try {
    await rateLimiter.wait()
    const response = await bluefinExchange.getPoolTicks({ pool: poolAddress })
    
    if (response?.data) {
      let ticks: any[] = Array.isArray(response.data) ? response.data :
        (response.data as any).ticks || Object.values(response.data).find(Array.isArray)
      
      return ticks || []
    }
    
    return []
  } catch (error) {
    log(`Error fetching Bluefin ticks: ${error}`)
    return []
  }
}

// Update main data collection function
async function collectData() {
  try {
    const timestamp = new Date();
    const collectedData: Record<string, any> = {};
    
    // Collect data for each pair
    for (const pair of Object.keys(POOL_ADDRESSES.CETUS)) {
      const formattedPair = pair.replace('_', '/');
      log(`Collecting data for ${formattedPair}`, LogLevel.INFO);
      
      try {
        // Get Cetus pool data
        const cetusPoolAddress = POOL_ADDRESSES.CETUS[pair as keyof typeof POOL_ADDRESSES.CETUS];
        const cetusData = await withRetry(
          () => getCetusPoolData(cetusPoolAddress, formattedPair),
          `Cetus ${formattedPair} data collection`
        );
        
        if (cetusData) {
          await savePoolData(cetusData);
          await savePriceData({
            timestamp,
            source: 'cetus',
            pair: formattedPair,
            price: cetusData.liquidity_usd / cetusData.liquidity,
            volume24h: cetusData.volume1h * 24,
            data_source: 'sdk'
          });
          collectedData[`cetus_${pair}`] = cetusData;
        }
        
        // Get Bluefin pool data
        const bluefinPoolAddress = POOL_ADDRESSES.BLUEFIN[pair as keyof typeof POOL_ADDRESSES.BLUEFIN];
        const bluefinData = await withRetry(
          () => getBluefinPoolData(bluefinPoolAddress, formattedPair),
          `Bluefin ${formattedPair} data collection`
        );
        
        if (bluefinData) {
          await savePoolData(bluefinData);
          await savePriceData({
            timestamp,
            source: 'bluefin',
            pair: formattedPair,
            price: bluefinData.liquidity_usd / (bluefinData.liquidity || 1),
            volume24h: bluefinData.volume24h || 0,
            data_source: 'api'
          });
          collectedData[`bluefin_${pair}`] = bluefinData;
        }
        
        // Calculate and save arbitrage opportunities
        if (cetusData && bluefinData) {
          const cetusPrice = cetusData.liquidity_usd / cetusData.liquidity;
          const bluefinPrice = bluefinData.liquidity_usd / (bluefinData.liquidity || 1);
          
          await saveArbitrageData(
            timestamp,
            formattedPair,
            'cetus',
            'bluefin',
            cetusPrice,
            bluefinPrice
          );
          
          log(`Saved arbitrage data for ${formattedPair}`, LogLevel.INFO);
        }
      } catch (error) {
        log(`Error collecting data for ${formattedPair}:`, LogLevel.ERROR, error as Error);
        // Continue with next pair
        continue;
      }
    }
    
    log('Data collection cycle completed successfully', LogLevel.INFO);
    return collectedData;
  } catch (error) {
    log('Error in data collection cycle:', LogLevel.ERROR, error as Error);
    return null;
  }
}

// Update startDataCollection function
async function startDataCollection() {
  log('Starting enhanced SUI arbitrage data collection', LogLevel.INFO);
  
  try {
    // Ensure all tables exist
    await ensureTablesExist();
    
    // Initial data collection
    const initialResult = await collectData();
    if (initialResult) {
      lastSuccessfulCollection = Date.now();
      log('Initial data collection completed successfully', LogLevel.INFO);
    }
    
    // Set up interval for continuous collection
    setInterval(async () => {
      try {
        const now = Date.now();
        const timeSinceLastCollection = now - lastSuccessfulCollection;
        
        if (timeSinceLastCollection < MIN_COLLECTION_INTERVAL) {
          log(`Waiting ${(MIN_COLLECTION_INTERVAL - timeSinceLastCollection) / 1000}s before next collection`, LogLevel.DEBUG);
          return;
        }
        
        const result = await collectData();
        if (result) {
          lastSuccessfulCollection = Date.now();
          log('Scheduled data collection completed successfully', LogLevel.INFO);
        }
      } catch (error) {
        log('Error in scheduled data collection:', LogLevel.ERROR, error as Error);
      }
    }, 5000); // Check every 5 seconds
    
    log('Data collection scheduler initialized successfully', LogLevel.INFO);
  } catch (error) {
    log('Fatal error in data collection setup:', LogLevel.ERROR, error as Error);
    process.exit(1);
  }
}

// Start the enhanced data collection
startDataCollection().catch(error => {
  log('Fatal error in main process:', LogLevel.ERROR, error as Error);
  process.exit(1);
}); 