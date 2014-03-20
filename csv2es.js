var csv = require('csv');
var argv = require('minimist')(process.argv.slice(2));
var _ = require('lodash');
var fs = require('fs');
var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'trace',
  apiVersion: '1.0'
});


console.log(argv);

var seperator = argv.s || argv.seperator || ",";
var headers = argv.H || argv.headers || null;

var opts = {};

var fromStream = argv._[0] ? fs.createReadStream(argv._[0]) : process.stdin;

var index = argv.i || argv.index || "csv2es";
var type = argv.t || argv.type || "csv2es";

var bulkSize = argv.b || argv.bulk || 1000;

var bulk = [];

console.log("reading");
csv()
    .from(fromStream, { delimiter: seperator, escape:'' })
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
    .on('record', function(row,index){
      bulk.push({ index:  { } })
      bulk.push(row);
      console.log("curbulk", bulk)
      if (bulk.length > 1000) {
        console.log("writing 1000 elements to ES")
        client.bulk({
          body: bulk,
          refresh: false,
          type: type,
          index: index
        }, function (err, resp) {
          console.log("ES err", err);
          console.log("ES resp", resp);
        });
        bulk = [];

      }

    })
    .on('end', function(){
      console.log("writing " + bulk.length + " elements to ES");
      client.bulk({
        body: bulk,
          refresh: false,
          type: type,
          index: index
      }, function (err, resp) {
        console.log("ES err", err);
        console.log("ES resp", resp);
      });
      bulk = []
    })
//    .on('data', function(d) {
//
//    })
    .on('error', function(error){
      console.log(error.message);
    });
