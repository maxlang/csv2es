var csv = require('csv');
var argv = require('minimist')(process.argv.slice(2));
var _ = require('lodash');
var fs = require('fs');
var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: 'http://localhost:9200',
  log: 'error',
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
csv()
    .from(fromStream, { delimiter: seperator, quote:''})
//.to.stream(process.stdout)
    .transform( function(row){
      if (!headers) {
        headers = row;
        return;
//  headers =  _.times(row.length, function(n) { return "field_" + n;})
      }
//      console.log("rr", row)
      return _.object(headers, row);
    })
    .on('record', function(row,i){
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
          if (pending === 0 && done) {
            process.exit()
          }
        });
        bulk = [];

      }

    })
    .on('end', function(){
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
        if (pending === 0) {
          process.exit()
        } else {
          done = true;
        }

      });
      bulk = []
    })
//    .on('data', function(d) {
//
//    })
    .on('error', function(error){
      console.log("node error", error.message);
    });
