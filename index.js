import Big from "big.js";
import WebSocket from "ws";
import fetch from 'node-fetch';
import { S3Client } from "@aws-sdk/client-s3";
import { Upload } from '@aws-sdk/lib-storage';
import { JsonStreamStringify } from 'json-stream-stringify';
import zlib from 'zlib';
import dotenv from 'dotenv';
import crc32 from 'crc/crc32';
dotenv.config();

// Parse parameters
const [ _base, _quote ] = process.argv.slice(2);
const base = _base.toUpperCase();
const quote = _quote.toUpperCase();

const is_test = (process.env.NODE_ENV === 'TEST');

const client = new S3Client({
  region: process.env['AWS-S3_REGION'], 
  credentials: {
    accessKeyId: process.env['AWS-S3_ACCESS_KEY_ID'],
    secretAccessKey: process.env['AWS-S3_SECRET_ACCESS_KEY'],
  }
});

const Market = [ base, quote ].join('');
let _Market = null;
let MarketWS = null;
let decimals = { price: null, amount: null };
const no_delay_attemps = 3;
const reconn_delay = 15e3;
const book_resync_time = 5*60e3; // 5 min
const max_snapshot_depth = 500;
const to_save_depth = is_test ? 5 : 50;
const seconds_to_export = 3600;
const num_of_connections = 3;

let connections = [];
let connections_size = 0;

const dict = {
  bside: { 'b': 'bids', 'a': 'asks', 'bs': 'bids', 'as': 'asks' },
  snapshot: { 'b': false, 'a': false, 'bs': true, 'as': true },
  bcmp: { 'bids': 'gte', 'asks': 'lte' }
};

const asset_translation = {
  "XBT": "BTC",
  "XDG": "DOGE"
};

const full_market_name =  `kraken-spot ${ (asset_translation[base] || base) }-${ ( asset_translation[quote] || quote) }`;

let data_time = null;
let seconds_data = [];

// 'console.log' start printing the current time (UTC-3).
let dlog = console.log;
console.dlog = dlog;
console.log = (...args) => {
  const ts = Date.now()-60e3*60*3;
  const strtime = new Date(ts).toLocaleString('pt-BR', { timeZone: 'UTC' }).replace(',', '') + '.' + (ts%1e3+'').padStart(3, 0);
  return dlog(...[ strtime, '-', ...args ]);
};

async function CompressAndSendBigJSONToS3 (name, bigjson) {
  const compress_stream = zlib.createGzip();
  new JsonStreamStringify(bigjson).pipe(compress_stream);

  const upload = new Upload({
    client,
    params: {
      Bucket: process.env['AWS-S3_BUCKET_NAME'],
      Key: name+'.gz',
      Body: compress_stream,
    }
  });

  await upload.done();
}

async function Request (path) {
  const r = await fetch('https://api.kraken.com' + path);

  if (r.status !== 200)
    throw '[E] Requesting "' + path + '": (' + r.status + ') ' + await r.text();

  return r.json();
}

async function ValidateMarket () {
  const tickers = Object.entries((await Request('/0/public/AssetPairs')).result);
  const [ _market, ticker ] = tickers.find(t => t[1].altname === Market);

  // Check if it is a valid market.
  if (ticker == null)
    throw '[E] ValidateMarket: Unknow "' + Market + '" market.';

  // Check if market is active.
  if (ticker.status !== 'online')
    throw '[E] ValidateMarket: "' + Market + '" market is suspended.'
  
  _Market = _market;
  MarketWS = ticker.wsname;
  decimals.price = ticker.cost_decimals;
  decimals.amount = ticker.lot_decimals;
}

function NewUpdateSec (conn, update_time, update_sec) {
  if (data_time == null) data_time = update_sec - 1;
  
  if (!is_test)
    console.log('[!] New second, book_sec ('+Math.floor(conn.orderbook.timestamp / 1e3)+') upd_sec ('+update_sec+') { '+((Date.now() - update_time) / 1e3).toFixed(3)+' sec delay }');

  while (data_time < update_sec) {
    const obj = {
      asks: conn.orderbook.asks.slice(0, to_save_depth),
      bids: conn.orderbook.bids.slice(0, to_save_depth),
      book_timestamp: conn.orderbook.timestamp,
      second: ++data_time,
      trades: conn.trades
        .filter(t => 
          Big(t.timestamp).gt((data_time - 1) * 1e3) &&
          Big(t.timestamp).lte(data_time * 1e3)
        )
        .map(trade => {
          const { custom_id, ...t } = trade;
          return t;
        })
      ,
    };
    
    if (is_test)
      console.log(obj);
    else
      seconds_data.push(obj);
  }

  if (data_time % seconds_to_export == 0) SaveToS3();
}

async function SaveToS3 () {
  if (is_test) return;
  
  // Create a name to the file being saved.
  const timestr = new Date((data_time - 60*60*3) * 1e3).toISOString().slice(0, 16).replaceAll(':', '-');
  const name =  full_market_name.replace(' ', '_') + '_' + timestr + '.json';

  // Compress data then save it.
  CompressAndSendBigJSONToS3(name, seconds_data)
    .then(() => console.log('[!] Data saved successfuly.'))
    .catch(error => console.log('[E] Failed to save data:',error));
  
  // Reset data in memory. 'seconds_data'.
  seconds_data = [];
}

class Connection {
  constructor () {
    this.conn_id = connections_size++;
    this.failed_attemps = 0;
  }

  async GetInitialTrades () {
    // console.log('Geting initial trades...');
  
    // Get initial trades from REST.
    let all_trades = [];
    let since = Math.floor(Date.now() / 1e3) - 5;
    while (true) {
      const r = await Request('/0/public/Trades?pair=' + Market + '&since=' + since + '&count=1000');
      const _trades = r.result[_Market];
      all_trades = [ ...all_trades, ..._trades ];
      if (_trades.length < 1000) break;
      since = Math.floor(_trades[_trades.length - 1][2]) - 1;
    }
  
    let init_trades = {};
    for (const t of all_trades) {
      const c_id_idx = t.push(t.slice(0, 5).join('|')) - 1;
      if (!init_trades[t[c_id_idx]]) init_trades[t[c_id_idx]] = t;
    }
  
    this.trades = Object.values(init_trades).map(t => ({
      timestamp: Math.floor(t[2] * 1e3),
      is_buy: (t[3] === 'b'),
      price: Big(t[0]).toFixed(),
      amount: Big(t[1]).toFixed(),
      custom_id: t[t.length-1]
    }));
  
    while (this.trades_upd_cache?.length > 0) {
      const trade = this.trades_upd_cache.shift();
      if (this.trades.findIndex(t => t.custom_id === trade.custom_id) === -1)
        this.trades.push(trade);
    }
  
    // console.log('[!] Got initial trades.',this.trades);
  }

  async SyncMarket () {
    this.syncProm = new Promise((resolve, reject) => { 
      this.syncPromFuncs = { resolve, reject };
    })
    .finally(() => {
      delete this.syncPromFuncs;
      delete this.syncProm;
    });
    
    this.last_conn_attemp = Date.now();
    const ws = new WebSocket("wss://ws.kraken.com");
  
    ws.on('open', () => {
      console.log('[!] (' + this.conn_id + ') WebSocket opened.');

      // Keep the connection alive with ping loop.
      this.pingLoopInterval = setInterval(() => ws.ping(), 30e3);

      // Subscribe to trades.
      ws.send(JSON.stringify({
        event: "subscribe",
        pair: [ MarketWS ],
        subscription: { name: "trade" },
      }));

      // Subscribe to orderbook.
      ws.send(JSON.stringify({
        event: "subscribe",
        pair: [ MarketWS ],
        subscription: { name: "book", depth: max_snapshot_depth }
      }));
    });
    
    ws.on('error', err => {
      console.log('[E] (' + this.conn_id + ') WebSocket:',err);
      ws.terminate();
    });
    
    ws.on('close', async () => {
      this.trades = null;
      this.orderbook = null;
      clearInterval(this.pingLoopInterval);

      console.log('[!] (' + this.conn_id + ') WebSocket closed.');

      if (Date.now() - this.last_conn_attemp < reconn_delay && ++this.failed_attemps > no_delay_attemps) {
        console.log('/!\\ (' + this.conn_id + ') Trying reconnection in ' + Math.ceil(reconn_delay / 1e3) + ' second(s).');
        await new Promise(r => setTimeout(r, reconn_delay));
      }

      console.log('/!\\ (' + this.conn_id + ') Reconnecting with WebSocket...');
      this.SyncMarket();
    });
  
    ws.on('ping', data => ws.pong(data));
    
    ws.on('message', msg => {
      try {
        // msg = msg.toString().replace(/:\s*([-+Ee0-9.]+)/g, ': "$1"'); // Convert numbers to string.
        msg = JSON.parse(msg);
      } catch (error) {
        msg = msg.toString();
      }

      if (Array.isArray(msg)) {
        if (msg[msg.length - 2] === 'book-500') {
          for (const update of msg.slice(1, -2)) {
            for (const key of Object.keys(update)) {
              if (key !== 'c') {
                // Side update
                const side = dict.bside[key];
                const snapshot = dict.snapshot[key];

                if (snapshot) {
                  if (this.orderbook == null) this.orderbook = {};
                  
                  this.orderbook[side] = update[key].map(([ p, q, ts ]) => {
                    ts = Math.floor(ts * 1e3);
                    
                    if (ts > (this.orderbook.timestamp || 0)) 
                      this.orderbook.timestamp = ts;

                    return [ Big(p).toFixed(), Big(q).toFixed() ];
                  });

                  this.orderbook.last_snapshot_time = Date.now();
                  console.log('[!] (' + this.conn_id + ') Book "' + side + '" snapshot applied.');

                } else {
                  if (this.orderbook == null) return;

                  for (const upd of update[key]) {
                    let [ price, amount, ts, republished ] = upd;
                    if (republished) continue;

                    price = Big(price);
                    amount = Big(amount);
                    ts = Math.floor(ts * 1e3);

                    // Check if is a new second.
                    const update_time = ts;
                    const update_sec = Math.floor(update_time / 1e3);
                    const book_sec = Math.floor(this.orderbook.timestamp / 1e3);
                    
                    if (this.trades != null && update_sec > book_sec && (data_time == null || update_sec > data_time))
                      NewUpdateSec(this, update_time, update_sec);

                    const idx = this.orderbook[side].findIndex(pl => price[dict.bcmp[side]](pl[0]));

                    if (amount.eq(0)) {
                      if (idx != -1 && price.eq(this.orderbook[side][idx][0])) {
                        // Remove price level.
                        this.orderbook[side].splice(idx, 1);
                      }
                    } else {
                      if (idx == -1) {
                        // Push to the end of the book side.
                        this.orderbook[side].push([ price.toFixed(), amount.toFixed() ]);

                      } else if (price.eq(this.orderbook[side][idx][0])) {
                        // Updates existing price level.
                        this.orderbook[side][idx][1] = amount.toFixed();

                      } else {
                        // Push adds new price level.
                        this.orderbook[side].splice(idx, 0, [ price.toFixed(), amount.toFixed() ]);

                      }
                    }

                    if (ts > (this.orderbook.timestamp || 0))
                      this.orderbook.timestamp = ts;
                  }
                }
              } else {
                // Checksum.
                if (this.orderbook == null) return;

                const asks_str = this.orderbook.asks
                  .slice(0, 10)
                  .reduce((s, [ p, q ]) => 
                    s + parseInt(Big(p).toFixed(decimals.price).replace('.', '')).toFixed() + parseInt(Big(q).toFixed(decimals.amount).replace('.', '')).toFixed()
                  , '');

                const bids_str = this.orderbook.bids
                  .slice(0, 10)
                  .reduce((s, [ p, q ]) => 
                    s + parseInt(Big(p).toFixed(decimals.price).replace('.', '')).toFixed() + parseInt(Big(q).toFixed(decimals.amount).replace('.', '')).toFixed()
                  , '');

                const checksum_str = asks_str + bids_str;
                const crc32_result = crc32(checksum_str);

                if (!Big(crc32_result).eq(update.c)) {
                  // Resync orderbook.
                  console.log('[E] (' + this.conn_id + ') wrong book, crc32_result (' + crc32_result + ') != update.c (' + update.c + '):');
            
                  dlog(' \n' + this.orderbook.asks.slice(0, 10).reverse().map(([p, q]) => p + '\t' + q).join('\n'),' \n');
                  dlog(this.orderbook.bids.slice(0, 10).map(([p, q]) => p + '\t' + q).join('\n'),' \n');
                  dlog(this.orderbook.timestamp,' \n');
            
                  console.log('Last update message processed:');
                  for (const _upd of msg.slice(1, -2)) dlog(_upd);
                  
                  console.log('/!\\ (' + this.conn_id + ') Resyncing orderbook...');
                  
                  this.orderbook = null;
                  ws.send(JSON.stringify({
                    event: "subscribe",
                    pair: [ MarketWS ],
                    subscription: { name: "book", depth: max_snapshot_depth }
                  }));

                  return;
                }
              }
            }
          }

          // Check if is time to resync the orderbook.
          if (this.orderbook._resyncing !== true && Date.now() - this.orderbook.last_snapshot_time > book_resync_time) {
            console.log('/!\\ (' + this.conn_id + ') Resyncing book...');
            this.orderbook._resyncing = true;
            ws.send(JSON.stringify({
              event: "subscribe",
              pair: [ MarketWS ],
              subscription: { name: "book", depth: max_snapshot_depth }
            }));
          }

          return;
        }
        
        if (msg[msg.length - 2] === 'trade') {
          if (this.trades == null) {
            if (!this.trades_upd_cache) this.trades_upd_cache = [];

            for (const raw_trade of msg.slice(1, -2)[0]) {
              const custom_id = raw_trade.slice(0, 5).join('|');
              if (!this.trades_upd_cache.some(t => t.custom_id === custom_id)) {
                this.trades_upd_cache.push({
                  timestamp: Math.floor(raw_trade[2] * 1e3),
                  is_buy: (raw_trade[3] === 'b'),
                  price: Big(raw_trade[0]).toFixed(),
                  amount: Big(raw_trade[1]).toFixed(),
                  custom_id
                });
              }
            }
          } else {
            for (const raw_trade of msg.slice(1, -2)[0]) {
              const custom_id = raw_trade.slice(0, 5).join('|');
              if (!this.trades.some(t => t.custom_id === custom_id)) {
                this.trades.push({
                  timestamp: Math.floor(raw_trade[2] * 1e3),
                  is_buy: (raw_trade[3] === 'b'),
                  price: Big(raw_trade[0]).toFixed(),
                  amount: Big(raw_trade[1]).toFixed(),
                  custom_id
                });
              }
            }
          }

          return;
        }
      }

      if (msg.event === 'subscriptionStatus' && msg.status === 'subscribed') {
        if (msg?.subscription?.name === 'trade') {
          console.log('[!] (' + this.conn_id + ') Subscribed to trades.');
          this.GetInitialTrades();
          return;
        }
        
        if (msg?.subscription?.name === 'book') 
          return console.log('[!] (' + this.conn_id + ') Subscribed to orderbook.');
      }

      if (msg.event === 'systemStatus' && msg.status === 'online') return;

      if (msg.event === 'heartbeat') return;

      console.log('/!\\ (' + this.conn_id + ') WebSocket message unhandled:',msg);
    });
  
    return this.syncProm;
  };
};

(async () => {
  await ValidateMarket();
  console.log('[!] Market is valid.');

  // Create and sync with 3 connections
  while (connections_size < num_of_connections)
    connections.push(new Connection());

  // Sync with the market using all the connections.
  await Promise.all(connections.map(c => c.SyncMarket()));
})();