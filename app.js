const _ = require('lodash');
const async = require('async');
const es = require('elasticsearch');
const fs = require('fs');
const request = require('request');

const configs = require('./configs').configs;

configs.forEach(function(config) {
  const esClient = new es.Client({
    host: config.es.host,
    requestTimeout: 60000
  });

  if (config.rest.contextFilename) {
    fs.readFile(config.rest.contextFilename, function(readErr, data) {
      let prevContext = null;
      if (readErr) {
        console.warn(`${config.es.index}: Unable to read context file, error: ${readErr}`);
      } else {
        try {
          console.info(`${config.es.index}: Loaded previous context, ${data}`);
          prevContext = JSON.parse(data);
        } catch (parseErr) {
          console.warn(`${config.es.index}: Unable to parse context file, error: ${parseErr}`);
        }
      }
      rest2es(prevContext, esClient, config);
    });
  } else {
    rest2es(null, esClient, config);
  }
});

function rest2es(prevContext, esClient, config) {
  const nextUrl = config.rest.next(prevContext);
  if (nextUrl) {
    http2es(nextUrl, esClient, config, function(err, newContext, numLoaded) {
      let wait = 0;
      let context = newContext;
      if (err) {
        console.warn(`${config.es.index}: Unable to pull data from ${nextUrl}. Error: ${err}. Sleeping before next poll`);
        wait = config.rest.sleep;
        context = prevContext;
      } else if (numLoaded === 0) {
        console.info(`${config.es.index}: Received no results on last poll attempt, sleeping before next poll`);
        wait = config.rest.sleep;
        context = prevContext;
      } else if (newContext && config.rest.contextFilename) {
        fs.writeFile(config.rest.contextFilename, JSON.stringify(newContext, null, ' '), function(writeErr) {
          if (writeErr)
            console.warn(`${config.es.index}: Unable to write context to file, error: ${writeErr}`);
        });
      }
      setTimeout(
        function() {
          rest2es(context, esClient, config);
        },
        wait
      );
    });
  } else {
    console.info(`${config.es.index}: nextUrl returned null, sleeping before next poll`);
    setTimeout(
      function() {
        rest2es(prevContext, esClient, config);
      },
      config.rest.sleep
    );
  }
}

/**
 * Pull data from http endpoint
 * tranform each document into ES document
 * Track min/max values of watched parameters (context for next request)
 * bulk load docs into ES
 */
function http2es(url, esClient, config, callback) {
  let bulk = [];
  const respContext = context_factory(config.rest.watchlist);
  console.info(`${config.es.index}: Polling data from ${url}`);
  request(url, function(requestErr, resp, body) {
    if (requestErr) {
      callback(requestErr);
      return;
    }
    if (resp.statusCode !== 200) {
      callback(resp.statusCode);
      return;
    }

    let rows = [];
    try {
      rows = config.extractRows(JSON.parse(body));
      console.info(`${config.es.index}: received ${rows.length} documents`);
    } catch (parseErr) {
      callback(`Unable to parse response body, error: ${parseErr}`);
      return;
    }

    let count = 0;
    async.forEachLimit(rows, 1,
      function(item, forEachCallback) {
        count++;
        config.transform(item, function(esDoc) {
          respContext.update(esDoc);
          const insertCmd = {
            index: {
              _index : config.es.index,
              _type: config.es.type
            }
          };
          if (esDoc._id) {
            insertCmd.index._id = esDoc._id;
            delete esDoc._id;
          }
          bulk.push(insertCmd);
          bulk.push(esDoc);
          if (bulk.length >= 200) {
            bulkLoad(config.es.index, esClient, bulk, function(bulkErr) {
              bulk = [];
              forEachCallback(bulkErr);
            });
          } else {
            forEachCallback();
          }
        });
      },
      function(forEachErr) {
        if (forEachErr) {
          callback(forEachErr);
          return;
        }
        bulkLoad(config.es.index, esClient, bulk, function(bulkErr) {
          callback(bulkErr, respContext.getContext(), count);
        });
      }
    );
  });
}

function bulkLoad(indexName, esClient, bulk, callback) {
  if (bulk.length === 0) {
    callback();
    return;
  }

  const numLoaded = bulk.length/2;
  console.info(`${indexName}: Starting bulk load, num documents: ${numLoaded}`);
  esClient.bulk(
    {
      body: bulk
    },
    function(esErr, resp) {
      if (esErr) {
        callback(`bulk load failed: ${esErr}`);
      } else if (resp.errors) {
        callback(`bulk load failed: ${JSON.stringify(resp.errors, null, '')}`);
      } else {
        console.info(`${indexName}: bulk load complete`);
        callback();
      }
    }
  );
}

function context_factory(watchlist) {
  return {
    context: {},
    getContext: function() {
      return this.context;
    },
    watchlist: watchlist,
    update: function(item) {
      const _this = this;
      this.watchlist.forEach(function(key) {
        if (_.has(item, key)) {
          const newValue = _.get(item, key);
          if (_.has(_this.context, key)) {
            //update min and max as appropiate
            const stored = _.get(_this.context, key);
            if (newValue < stored.min) {
              stored.min = newValue;
            }
            if (newValue > stored.max) {
              stored.max = newValue;
            }
          } else {
            //first time key seen - init min and max
            _this.context[key] = {
              min: newValue,
              max: newValue
            }
          }
        }
      });
    }
  }
}