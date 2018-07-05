const bch = require('bitcoincashjs')
const RpcClient = require('bitcoind-rpc');
const zmq = require('zeromq');
const async = require('async');
const Bit = {
  threshold: 5,
  GENESIS: 525470,  // OP_RETURN GENESIS
  config: {},
  init: function(config, callback) {
    // Initialize DB
    if (config) {
      if (config.on) {
        Bit.handlers = config.on;
      }
      if (config.syncFrom) {
        Bit.synchronizer = config.syncFrom;
      }
      if (config.rpc) {
        Bit.config.rpc = config.rpc;
      }
      if (config.zmq) {
        Bit.config.zmq = config.zmq;
      }
      Bit.rpc = new RpcClient(config.rpc);
    }
    callback()
  },

  /**
   * Start listening
   * 1. new hashtx: Add to unconfirmed collection
   * 2. new hashbblock: sync() to synchronize to the latest block
   */
  listen: function() {
    var sock = zmq.socket('sub');
    sock.connect('tcp://' + Bit.config.zmq.host + ':' + Bit.config.zmq.port);
    sock.subscribe('hashtx');
    sock.subscribe('hashblock');
    console.log('Subscriber connected to port ' + Bit.config.zmq.port);

    var unconfirmedQueue = async.queue(function(hash, callback) {
      Bit.getUnconfirmedItems(hash, function(opReturns) {
        Bit.handlers.mempool(opReturns)
        callback()
      })
    }, Bit.threshold)
    unconfirmedQueue.drain = function() {
      console.log('unconfirmed queue empty');
    };

    sock.on('message', function(topic, message) {
      if (topic.toString() === 'hashtx') {
        let hash = message.toString('hex')
        unconfirmedQueue.push(hash)
      } else if (topic.toString() === 'hashblock') {
        Bit.sync(function() {
          console.log("Block Synchronized");
        })
      }
    });
  },

  /**
   * Crawl confirmed blocks from block index 'start' to 'end'
   */
  crawl: function(start, end, callback) {
    var index = (start ? start : Bit.GENESIS);
    Bit.getBlockHashes(index, end, [], function(blockHashes) {
      console.log("> got block hashes from ", index, " to ", end)
      Bit.getTransactionHeaders(blockHashes, 0, [], function(transactionHeaders) {
        Bit.getConfirmedItems(transactionHeaders, function() {
          console.log("finished getOpReturn")
          callback()
        })
      });
    })
  },

  /**
   * Synchronize Bit.
   * Find the most recently synchronized block and start crawling until the current block
   */
  sync: function(callback) {
    // 1. Get the most recent block
    Bit.rpc.getBlockCount(function(err, res) {
      if (err) { throw err; }
      else {
        // 2. Find the last synchronoized block by stepping backwards until we find one
        var index = res.result;
        console.log("block count = ", index);
        Bit.synchronizer(function(lastSynchronized) {
          console.log("last synchronized block = ", lastSynchronized);
          if (!lastSynchronized) {
            lastSynchronized = Bit.GENESIS;
          }
          // 3. Last synchronized block found. Start synchronizing from the block right after it
          if (index > lastSynchronized) {
            // The last synchronized block is less than the current block
            Bit.rpc.getBlockCount(function(err, latestBlockCount) {
              // 4. Start crawling from right after the lastSynchromized block
              Bit.crawl(lastSynchronized + 1, latestBlockCount.result, function() {
                console.log("Synchronized from " + (lastSynchronized+1) + " to " + latestBlockCount.result);
                callback();
              })
            })
          } else {
            // The last synchronized block is the same as the current block => All synchronized
            console.log("already synchronized to " + lastSynchronized);
            callback();
          }
        })
      }
    })
  },

  /**
   * Takes an array of buffer objects and check if they're valid OP_RETURNs or not
   * @param {array} chunks - An array of buffer to check
   * @returns {bookean} - whether this sequence is OP_RETURN or not
   */
  isvalid_opreturn: function(chunks) {
    if (chunks && chunks.length > 1 && chunks[0].opcodenum === 106) {
      return true;
    }
    return false;
  },


  /**
   * Takes a single transaction hash, transforms it into desired format, and returns via callback
   * @param {string} transactionHash - transaction hash
   * @param {function} callback - the callback function to return the transformed opreturn item
   */
  getUnconfirmedItems: function(transactionHash, callback) {
    var ephemeralOpReturns = [];
    Bit.rpc.getRawTransaction(transactionHash, function(err, transaction) {
      if (err) {
        console.log("Error: ", err)
      } else {
        var t = new bch.Transaction(transaction.result);
        let senders = [];
        let senderAddresses = [];
        t.inputs.forEach(function(input, input_index) {
          if (input.script) {
            let address = input.script.toAddress(bch.Networks.livenet).toString(bch.Address.CashAddrFormat).split(':')[1];
            senders.push({
              a: address
            });
            senderAddresses.push(address)
          }
        })
        let receivers = [];
        t.outputs.forEach(function(output, output_index) {
          console.log("Script = ", output.script)
          if (output.script) {
            if (output.script.isPublicKeyHashOut()) {
              let address = output.script.toAddress(bch.Networks.livenet).toString(bch.Address.CashAddrFormat).split(':')[1];
              receivers.push({
                a: address,
                v: output.satoshis
              });
            }
            // Only return opreturn transactions
            if (Bit.isvalid_opreturn(output.script.chunks)) {
              var data = output.script.chunks.slice(1)
              const item = {
                data: data,   // the output data
                index: output_index,  // the output index within the transaction
                tx: t.hash,
                senders: senders,
                receivers: receivers
              }
              ephemeralOpReturns.push(item);
            }
          }
        })
      }
      callback(ephemeralOpReturns);
    })
  },

  /**
   * Fetches an array of full blocks, given an array of block hashes
   * @param {array} transactionHeaders - array of transaction header objects
   * @param {function} callback - the callback function to return the array of full blocks
   */
  getConfirmedItems: function(transactionHeaders, callback) {

    // Asynchronously fetch raw transactions and construct OP_RETURN objects
    async.parallelLimit(
      transactionHeaders.map(function(transactionHeader) {
        return function(header, cb) {
          Bit.rpc.getRawTransaction(header.hash, function(err, transaction) {
            if (!err) {
              let t = new bch.Transaction(transaction.result);
              let senders = [];
              let senderAddresses = [];
              t.inputs.forEach(function(input, input_index) {
                if (input.script) {
                  var address = input.script.toAddress(bch.Networks.livenet).toString(bch.Address.CashAddrFormat).split(':')[1];
                  if (address) {
                    senders.push({ a: address });
                    senderAddresses.push(address)
                  }
                }
              })
              let receivers = [];
              t.outputs.forEach(function(output, output_index) {
                // Out address
                if (output.script) {
                  let address = output.script.toAddress(bch.Networks.livenet).toString(bch.Address.CashAddrFormat).split(':')[1];
                  if (address) {
                    if (senderAddresses.indexOf(address) < 0) {
                      receivers.push({
                        a: address, v: output.satoshis
                      });
                    }
                  }
                }
                if (Bit.isvalid_opreturn(output.script.chunks)) {
                  let data = output.script.chunks.slice(1)
                  let item = {
                    data: data,   // output data
                    index: output_index,  // output index within transaction
                    block: {
                      index: header.block_index,
                      hash: header.block_hash,
                      time: header.block_time
                    },
                    senders: senders,
                    receivers: receivers,
                    tx: t.hash
                  }
                  Bit.handlers.block.queue(item)
                }
              })  // end of t.outputs.forEach
            }
            cb()
          })
        }.bind(null, transactionHeader)
      }),
      Bit.threshold,
      function() {
        // All finished queuing. Execute one last time
        console.log("Finished queueing");
        Bit.handlers.block.exec()
        callback()
      }
    )
  },

  /**
   * Given a start and end block index, return an array of block hashes betwen the range
   * @param {number} start - start index
   * @param {number} end - end index
   * @param {array} blockHashes - the array to iterative collect the hashes throughout the recursive calls
   * @param {function} callback - the callback function to return the array of full blocks
   */
  getBlockHashes: function(start, end, blockHashes, callback) {
    console.log("getBlockHashes", start, end);
    if (start <= end) {
      Bit.rpc.getBlockHash(start, function(err, res) {
        let hash = res.result;
        Bit.rpc.getBlock(hash, function(err, block) {
          blockHashes.push({ index: start, hash: hash, time: block.result.time })
          Bit.getBlockHashes(start+1, end, blockHashes, callback);
        })
      })
    } else {
      callback(blockHashes);
    }
  },

  /**
   * Fetches an array of full blocks, given an array of block hashes
   * @param {array} blockHashes - the array of block hashes
   * @param {number} index - the index of the block to process
   * @param {array} transactionHeaders - the array to collect the transaction headers throughout the recursive calls
   * @param {function} callback - the callback function to return the array of full blocks
   */
  getTransactionHeaders: function(blockHashes, index, transactionHeaders, callback) {
    if (index < blockHashes.length) {
      Bit.rpc.getBlock(blockHashes[index].hash, function(err, block) {
        block.result.tx.forEach(function(t) {
          transactionHeaders.push({
            hash: t,
            block_hash: blockHashes[index].hash,
            block_index: blockHashes[index].index,
            block_time: blockHashes[index].time
          })
        })
        console.log("getTransactionHeaders", index , " of ", blockHashes.length-1)
        Bit.getTransactionHeaders(blockHashes, index+1, transactionHeaders, callback);
      })
    } else {
      callback(transactionHeaders);
    }
  }
}
module.exports = {
  init: Bit.init,
  sync: Bit.sync,
  listen: Bit.listen
}
