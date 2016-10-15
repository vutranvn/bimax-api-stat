var restify = require('restify');
var redis = require('redis');
var moment = require('moment');
var when = require('when');
var async = require('async');
var _  = require('underscore');
var server = restify.createServer({
    name: 'myapp',
    version: '1.0.0'
});

var winston = require('winston');
winston.level = 'debug';
winston.add(winston.transports.File, { filename: '/var/log/app.log' });

var NodeCache = require( "node-cache" );
var cache = new NodeCache();


var rclient = redis.createClient("/tmp/redis_counters.sock");

rclient.on("error", function (err) {
    //console.log("Error " + err);
});

server.use(restify.acceptParser(server.acceptable));
server.use(restify.queryParser());
server.use(restify.bodyParser());

server.get('/api/v2/stat', function (req, res, next) {
    var params = req.params;
    var types = params['type'];
    var names = params['name'];
    var date = params['date'];
    var period= params['period'];
    var unit = params['unit'];
    if(!types || !names || !date || !period || !unit) {
        res.send({status: false, msg: 'Must have param type, name, date, period, unit'});
        return;
    }

})

server.listen(8002, function () {
});
