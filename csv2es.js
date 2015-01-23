var csv = require('csv');
var argv = require('minimist')(process.argv.slice(2));
var _ = require('lodash');
var fs = require('fs');
var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: 'http://localhost:9200',
  log: 'trace',
  apiVersion: '1.4'
});


console.log(argv);

var seperator = argv.s || argv.seperator || ",";
var headers = argv.H || argv.headers || null;

var opts = {};

var fromStream = argv._[0] ? fs.createReadStream(argv._[0]) : process.stdin;

var index = argv.i || argv.index || "csv2es";
var type = argv.t || argv.type || "csv2es";

var bulkSize = argv.b || argv.bulk || 1000;
var concurrent = argv.c || argv.concurrent || 5;

var bulk = [];

//var requestQueue = [];
//
//function performRequest()  {
//
//}

var pending = 0;
var done = false;

console.log("reading");

var parser = csv.parse({ delimiter: seperator, quote:''});

fromStream.on('readable', function() {
  while(data = fromStream.read()){
    console.log("read data", data);
    parser.write(data);
  }
  transformer.end();
});

var transformer = csv.transform(function(row) {
  console.log("transforming row", row);
  if (!headers) {
    headers = row;
    return;
  }
  return _.object(headers, row);
});

parser.on('readable', function(){
  while(data = parser.read()){
    console.log("parsed data", data);
    transformer.write(data);
  }
});

transformer.on('readable', function(){
  var row;
  while(row = transformer.read()){
    console.log("transformed data", row);
    bulk.push({ index:  { } });
    bulk.push(row);
//      console.log("curbulk", bulk)
    if (bulk.length / 2 > bulkSize) {
      pending ++;
      console.log("writing " + bulk.length/2 + " elements to ES");
      client.bulk({
        body: bulk,
        refresh: false,
        type: type,
        index: index
      }, function (err, resp) {
        if (err) {
          console.log("ES err", err);
        }
//          console.log("ES resp", resp);
        pending--;
        console.log("exit?", pending, done);
        if (pending <= 0 && done) {
          process.exit()
        }
      });
      bulk = [];

    }
  }
  console.log("transformer done");
});

    transformer.on('finish', function(){
  console.log("writing " + bulk.length/2 + " elements to ES");
      pending ++;
  client.bulk({
    body: bulk,
    refresh: false,
    type: type,
    index: index
  }, function (err, resp) {
    if (err) {
      console.log("ES err", err);
    }
//          console.log("ES resp", resp);
    pending--;
    console.log("exit?", pending);
    if (pending <= 0) {
      process.exit()
    } else {
      done = true;
    }

  });
  bulk = []
}) ;
//    .on('data', function(d) {
//
//    })
    transformer.on('error', function(error){
      console.log("node error", error.message);
    });
