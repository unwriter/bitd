var bit = require('./bit')
var db = require('./db')
db.init({
  dbName: "bitdb",
  url: 'mongodb://localhost:27017'
}, function() {
  bit.init({
    rpc: {
      protocol: 'http',
      user: 'root',
      pass: 'bitcoin',
      host: '127.0.0.1',
      port: '8332',
    },
    on: {
      mempool: function(opReturns) {
        db.process.mempool(opReturns, function() { })
      },
      block: {
        queue: function(r) {
          db.process.block.queue(r, function() { })
        },
        exec: function() {
          db.process.block.exec(function() { })
        }
      }
    },
    syncFrom: db.sync
  }, function() {
    console.time("Sync");
    bit.sync(function() {
      console.timeEnd("Sync");
      bit.listen()
    })
  })
})
