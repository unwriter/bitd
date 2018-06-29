const MongoClient = require('mongodb').MongoClient;
const iconv = require('iconv-lite');
var DB = {
  bulk: {
    confirmed: null,
    unconfirmed: null,
    counter: 0
  },
  threshold: 1000,
  process: {
    block: {
      queue: function(r, cb) {
        let item = {
          tx: r.tx,
          senders: r.senders,
          receivers: r.receivers,
          block_index: r.block.index,
          block_hash: r.block.hash,
          block_time: r.block.time
        }
        r.data.forEach(function(b, index) {
          if (b.buf) {
            item["b" + (index + 1)] = b.buf.toString('base64')
            item["s" + (index + 1)] = b.buf.toString('utf8')
          } else {
            item["b" + (index + 1)] = b;
            item["s" + (index + 1)] = b;
          }
        })
        // bulk schedule confirmed
        DB.bulk.confirmed.insert(item)
        DB.bulk.counter++;

        // bulk schedul unconfirmed
        DB.bulk.unconfirmed.find({tx: r.tx}).remove()

        if (DB.bulk.counter >= DB.threshold) {
          console.log("syncing block", item.block_index)
          DB.process.block.exec()
        }
        if (cb) cb()
      },
      /**
      * Flush and execute all tasks in a queue
      */
      exec: function(cb) {
        // flush unconfirmed transactions included in the current block

        if(DB.bulk && DB.bulk.unconfirmed && DB.bulk.unconfirmed.s && DB.bulk.unconfirmed.s.currentBatch && DB.bulk.unconfirmed.s.currentBatch.operations && DB.bulk.unconfirmed.s.currentBatch.operations.length > 0) {
          DB.bulk.unconfirmed.execute().then(function(result) {
            console.log("[unconfirmed collection] removed ", result.nRemoved)
          })
          DB.bulk.unconfirmed = DB._db.collection("unconfirmed").initializeUnorderedBulkOp();
          console.log("[unconfirmed collection]")
        }

        // update confirmed transactions batch
        if(DB.bulk && DB.bulk.confirmed && DB.bulk.confirmed.s && DB.bulk.confirmed.s.currentBatch && DB.bulk.confirmed.s.currentBatch.operations && DB.bulk.confirmed.s.currentBatch.operations.length > 0) {
          DB.bulk.confirmed.execute().then(function(result) {
            console.log("[confirmed collection] upserted", result.nInserted)
          });
          DB.bulk.confirmed = DB._db.collection("confirmed").initializeUnorderedBulkOp();
          DB.bulk.counter = 0;
          console.log("[confirmed collection]")
        } else {
          console.log("Nothing to update")
        }
        if (cb) cb()
      },
    },
    mempool: function(opReturns, cb) {
      console.log("getUnconfirmedItems ", opReturns)
      opReturns.forEach(function(r) {
        let item = {
          tx: r.tx,
          senders: r.senders,
          receivers: r.receivers,
        }
        r.data.forEach(function(b, index) {
          if (b.buf) {
            item["b" + (index + 1)] = b.buf.toString('base64')
            item["s" + (index + 1)] = b.buf.toString('utf8')
          } else {
            item["b" + (index + 1)] = b;
            item["s" + (index + 1)] = b;
          }
        })
        // Add to unconfirmed
        // Remove later when confirmed.
        DB._db.collection("unconfirmed").update(
          { tx: r.tx }, item, { upsert: true }
        ).then(function(result) {
          console.log("inserted", JSON.stringify(item, null, 2));
        })
      })
      cb()
    }
  },
  /**
   * Find the last synchronized block
   */
  sync: function(callback) {
    DB._db.collection("confirmed").find().sort({block_index:-1}).limit(1).toArray(function(err, items) {
      console.log(items)
      if (items.length > 0) {
        let last_item = items[0];
        console.log("Last synchronized = ", last_item.block_index);
        callback(last_item.block_index)
      } else {
        console.log("Last synchronized = GENESIS");
        callback()
      }
    })
  },
  exit: function() {
    DB.mongo.close()
  },
  init: function(config, callback) {
    DB.config = config;
    // Initialize
    DB.counter = 0;
    MongoClient.connect(DB.config.url, {useNewUrlParser: true}, function(err, client) {
      if (err) console.log(err)
      DB._db = client.db(DB.config.dbName);

      // Create text search index
//      DB._db.collection("confirmed").createIndex({"$**":"text"})
//      DB._db.collection("unconfirmed").createIndex({"$**":"text"})

      DB.mongo = client;
      DB.bulk.confirmed = DB._db.collection("confirmed").initializeUnorderedBulkOp();
      DB.bulk.unconfirmed = DB._db.collection("unconfirmed").initializeUnorderedBulkOp();
      callback()
    })
  }
}
module.exports = {
  init: DB.init,
  process: DB.process,
  confirmed: function() {
    return DB._db.collection("confirmed");
  },
  unconfirmed: function() {
    return DB._db.collection("unconfirmed");
  },
  sync: DB.sync
}

