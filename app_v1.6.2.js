var restify = require('restify');
var redis 	= require('redis');
var moment 	= require('moment');
var when 	= require('when');
var async 	= require('async');
var _  		= require('underscore');
var server 	= restify.createServer({
	name: 'myapp',
	version: '1.0.0'
});

var winston 	= require('winston');
winston.level 	= 'debug';
winston.add(winston.transports.File, { filename: '/var/log/app.log' });

var NodeCache 	= require( "node-cache" );
var cache 		= new NodeCache();


var rclient = redis.createClient(
	// "/tmp/redis_counters.sock"
	{host: "127.0.0.1", port: 6379}
);

rclient.on("error", function (err) {
	// console.log("Error " + err);
});

server.use(restify.acceptParser(server.acceptable));
server.use(restify.queryParser());
server.use(restify.bodyParser());
function redis_reget(rclient,key, unit_key, unit_type, level, acc_type){
	// winston.log('debug', "key:" + key + ' unit_key:' + unit_key + ' unit_type:' + unit_type );
	var defer = when.defer();
	var cache_key = key + unit_key + "+0700";

	var cache_val = cache.get(cache_key);
	// winston.log('debug', 'cache_key:' + cache_key + ' -> ' + cache_val);
	// winston.log('debug', 'cache_val:' + typeof(cache_val));
	if(cache_val != undefined) {
		defer.resolve(cache_val);
		return defer.promise;
	}
	var mkey = unit_key + "+0700";
	rclient.hget(key, mkey, function(err, rres){
	// winston.log("debug", "hget:" + mkey + " ret:" + rres );
		if(err) { defer.resolve(0);return;}
		else {
			var uu;
		switch(unit_type){
			case 'HOUR':
				uu = moment().format('YYYYMMDDHH');break;
			case 'DAY':
				uu = moment().format('YYYYMMDD');break;
			case 'MONTH':
				uu = moment().format('YYYYMM');break;
			case 'YEAR':
				uu = moment().format('YYYY');break;
			default:
				uu = moment().format('YYYYMMDDHHmm');break;
		}
		// winston.log('debug', 'level: ' + level + ' unit_type:' + unit_type + ' unit_key:' +  unit_key + ' vs ' + uu);
		if(rres) {
			// if(rres || rres == 0) {
			if(unit_key != uu) {
				if(level > 0 && unit_type != 'MINUTE') {
					rclient.hset(key, unit_key + "+0700", rres);
					// winston.log("debug", "hset:" + key + " -> "  + unit_key + "+0700" + " ret:" + rres );
				}
				// winston.log("debug", 'old cache put: ' + cache_key + ' -> ' + rres);
				cache.set(cache_key, rres);
			}
			// else {
				// winston.log("debug", 'today cache put: ' + cache_key + ' -> ' + rres);
				// cache.set(cache_key, rres, 60*1000);
			// }
			defer.resolve(rres);return;
		}
		if(unit_type == 'MINUTE') {
			defer.resolve(0); return;
		}

		var N = 60;
		var unit_type1;
		switch(unit_type){
			case 'HOUR':
				N = 60;	unit_type1 = 'MINUTE';break;
			case 'DAY':
				N = 24; unit_type1 = 'HOUR';break;
			case 'MONTH':
				N = 31; unit_type1 = 'DAY';break;
			case 'YEAR':
				N = 12; unit_type1 = 'MONTH'; break;
		}
		var datar = [];
		var timekey;
		var curt 	=  moment().format('YYYYMMDDHHmm');
		for(var j = 0; j < N;j ++) {
			var jj = j;
			if(j <= 9) jj = '0' + j;
				timekey = unit_key + jj;
			if(timekey <= curt)
				datar.push(redis_reget(rclient, key, unit_key + jj, unit_type1, level  + 1, acc_type));
		}
		when.all(datar).then(function(datai){
			var s = 0;
			if(acc_type == 'sum') {
				for(var k = 0; k < datai.length; k++) {
					s = s + parseInt(datai[k]);
				}
			} else if(acc_type == 'max') {
				for(var k = 0; k < datai.length; k++) {
					var sa = parseInt(datai[k]);
					if(sa > s) s = sa;
				}
			}
			// winston.log('debug', 's:' +  s);
			if(unit_key != uu) {
				if(unit_type != 'MINUTE') {
					rclient.hset(key, unit_key + "+0700",s);
				}
				cache.set(cache_key , s);
			} else {
				// cache.set(cache_key , s, 120*1000);
			}
			defer.resolve(s);

		 })
	}
	})
	return defer.promise;
}

function redis_get(rclient, key, keys, keys_origin, keys_origink, unit_type, acc_type, cb){
	rclient.exists(key, function(err, exists){
		// winston.log("debug","check exists:" + key + " ret:");
		if(err) { cb(null, []);return;};
		var datar = [];
		if(!exists) {
			for(var j = 0; j < keys_origink.length; j ++) {
				datar.push({name:keys_origink[j], value: 0});
			}
			cb(null, datar);
			return;
		}
		for(var j = 0; j < keys.length; j ++) {
			datar.push(redis_reget(rclient, key, keys[j], unit_type, 0 , acc_type));
		}
		when.all(datar).then(function(datai){
			var data = [];
			for(var jj = 0; jj < datai.length; jj++){
				if(!datai[jj]) datai[jj] = 0;
					data.push({name:keys_origink[jj], value: datai[jj]});
			}
			cb(null,data);
		})

	});

}
function bw2traf(val, unit) {
	switch(unit){
		case 'minute':
			t=60;break;
		case 'hour':
			t=3600;break;
			// t=60*60;break;
		case 'day':
			t=86400;break;
			// t=24*60*60;break;
		case 'month':
			t=2678400;break;
			// t=31*24*60*60;break;
		case 'year':
			t=31536000;break;
			// t=365*24*60*60;break;
	}
	return val*8/t;

}
server.get('/api/v1/stat', function (req, res, next) {
	winston.log('debug', req.query);
	var params 	= req.params;
	var types 	= params['type'];
	var names 	= params['name'];
	var date 	= params['date'];
	var period 	= params['period'];
	var unit 	= params['unit'];
	if(!types || !names || !date || !period || !unit) {
		res.send({status: false, msg: 'Must have param type, name, date, period, unit'});
		return;
	}
	var cache_total_key = JSON.stringify(params);
	var cache_total_val = cache.get(cache_total_key);
	// if(cache_total_val) {
	// 	var vv = JSON.parse(cache_total_val);
	// 	res.send({status: true, unit: unit, data: vv.data, total: vv.total});
	// 	return;
	// }
	var from,to,
	format = "YYYYMMDDHHmm";
	switch(period) {
		case "range":
			var date_range = date.split(",");
			if(date_range[0])
				from = moment(date_range[0]);
			else
				from = moment();
			if(date_range[1])
				to = moment(date_range[1]);
			else
				to = moment();
			break;
		default:
			to = moment(date);
			var period_range = period.split(" ");
			var tocl = to.clone();
			if(period_range.length == 1 || parseInt(period_range[0]) == 0)
				from = tocl;
			else {
				from = tocl.subtract(period_range[0], period_range[1]);
			}
			from.hour(0).minute(0).second(0);
	}
	var dd 		= from.clone();//.add(1, unit);
	var keys 	= [];
	var keys_origin 	= [];
	var keys_origink 	= [];
	var unit_type = "MINUTE";
	format 	= "YYYYMMDDHHmm";
	formatk = "YYYY-MM-DD HH:mm";
	var duration = moment.duration(to.diff(from));
	if(duration.asHours() >= 4 && /minute/.test(unit))
		unit = "hour"

	// winston.log("debug", "duration:" + duration.asHours());
	// winston.log("debug", "unit_type:" + unit_type);

	if(/hour/.test(unit)) {
		format 	= "YYYYMMDDHH";
		formatk = "YYYY-MM-DD HH";
		unit_type = "HOUR";
	} else if(/day/.test(unit)) {
		format 	= "YYYYMMDD";
		formatk = "YYYY-MM-DD";
		unit_type = "DAY";
	} else if(/month/.test(unit)) {
		format 	= "YYYYMM";
		formatk = "YYYY-MM";
		unit_type = "MONTH";
	} else if(/year/.test(unit)) {
		format 	= "YYYY";
		formatk = "YYYY";
		unit_type = "YEAR";
	}

	while(dd <= to) {
		var dj = dd.clone().format(format);
		keys.push(dj);
		keys_origin.push(dd.clone());
		keys_origink.push(dd.clone().format(formatk));
		dd = dd.add(1, unit);
	}
	var keys_arr 	= names.split(',');
	var type_arr1 	= types.split(',');

	var total 	= [];
	var totali 	= {};

	var type_arr = [];
	var type_avg_speed 	= false;
	var type_traffic_ps = false;
	_.each(type_arr1, function(tt){
		if(/avg_speed/.test(tt)) {
			type_arr.push(tt.replace('avg_speed', 'speed_request'));
			type_arr.push(tt.replace('avg_speed', 'request_count_2xx'));
			type_avg_speed = true;
		} else if(/traffic_ps/.test(tt)) {
			type_arr.push(tt.replace('traffic_ps','body_bytes_sent'));
			type_traffic_ps = true;
		} else {
			type_arr.push(tt);
		}
		/*
		switch(tt) {
			case 'avg_speed':
				type_arr.push('speed_request');
				type_arr.push('request_count_2xx');
				type_avg_speed = true;
				break;
			case 'traffic_ps':
				type_arr.push('body_bytes_sent');
				type_traffic_ps = true;
				break;
			default:
				type_arr.push(tt);
		}
		*/
	})
	var acc_type = 'sum';
	if(type_avg_speed) {
		acc_type = 'max';
	}
	var domain = [];
	async.map(keys_arr, function(key_each, type_callback){
		domain.push(key_each);
		async.map(type_arr, function(type, callback){
			// iteratee function of sub-async
			var key = type + '|' + key_each;
			/*
			winston.log("debug", "key_each:" + key_each);
			winston.log("debug", "type:" + type);
			winston.log("debug", "key:" + key);
			winston.log("debug", "key_origin:" + keys_origin);
			winston.log("debug", "key_origink:" + keys_origink);
			winston.log("debug", "unit_type:" + unit_type);
			*/

			redis_get(rclient, key, keys, keys_origin, keys_origink, unit_type, acc_type, function(err, data){
				if(err) {
					callback(err); return;
				} else {
					for(var ii = 0; ii < data.length; ii ++ ) {
						var ee = data[ii];
						var kk = type  + '|' + ee['name'];
						if(totali[kk]) {
							totali[kk] += parseInt(ee['value']);
						} else {
							totali[kk]  = parseInt(ee['value']);
						}
					}
					// winston.log("debug", "data:", data);
					callback(null, {type: type, value: data});
				}
			})
		}, function(err, result){
			// callback of sub-async
			if(type_avg_speed) {
				var t1 ,t2;
				var result1 = [];
				_.each(result, function(mo){
					var type = mo.type;
					if(/speed_request/.test(type))
					// if(mo.type == 'speed_request')
						t1 = mo.value;
					// else if(mo.type == 'request_count_2xx')
					else if(/request_count_2xx/.test(type))
						t2 = mo.value;
					else
						result1.push(mo);
				})
				var vva = _.map(t1, function(vv, ix){
					var kk = 0;
					if(t2[ix] && t2[ix].value) {
						kk = vv.value / t2[ix].value;
					}
					return {name: vv['name'], value: kk};
				})
				result1.push({type: 'avg_speed', value: vva});
				result = result1;
			}
			if(type_traffic_ps) {
				var result1 = [];
				_.each(result, function(mo){
					var type = mo.type;
					//if(mo.type == 'body_bytes_sent') {
					if(/body_bytes_sent/.test(type)) {
						var vva = _.map(mo.value, function(kkv){
							//kkv['value'] = kkv['value'] * 8 / 60;
							kkv['value'] = bw2traf(kkv['value'], unit);
							return kkv;
						})
						result1.push({type: 'traffic_ps', value: vva});

					} else {
						result1.push(mo);
					}
				})
				result = result1;
			}
			// winston.log("debug", "result: ", result);
			type_callback(null, {name: key_each, value: result});
		})
		}, function(type_err, type_result){
			// callback function of master-async
			var final_result = _.flatten(type_result);
			winston.log("debug", "final_result: ", final_result);
			winston.log("debug", "domain: ", domain);
			var ddata = {};
			var t1 = {}, t2 = {};
			Object.keys(final_result).forEach(function(kl1){
				var dom 		= final_result[kl1];
				var totalByName = [];
				Object.keys(dom).forEach(function(kl2){
					var typ = dom[kl2];
					Object.keys(typ).forEach(function(kl3){
						// winston.log("debug", "typ: ", typ);
						// var valueOfTotal = _.find(
						// 	totalByName,
						// 	function(col) {
						// 		if(col.name === ddatakk[kl3]['name'])
						// 			return col;
						// 	}
						// );
						// if ( valueOfTotal ) {
						// 	var index = totalByName.indexOf(valueOfTotal);
						// 	totalByName[index]['value'] = valueOfTotal['value'] + ddatakk[kl3]['value'];
						// } else
						// 	totalByName.push({name: ddatakk[kl3]['name'], value: ddatakk[kl3]['value']});
						// winston.log("debug", "valueOfTotal: ", valueOfTotal);
					})
				})

				total.push({name: dom['name'], value: dom['value']});
			})
			// Object.keys(totali).forEach(function(kk){
			// 	var ll = kk.split('|');
			// 	var tt = ll[0];
			// 	var dd = ll[1];
			// 	//switch(tt) {
			// 	//	case 'body_bytes_sent':
			// 		if(/body_bytes_sent/.test(tt)) {
			// 			if(type_traffic_ps) {
			// 				if(!ddata['traffic_ps']) ddata['traffic_ps'] = [];
			// 				ddata['traffic_ps'].push({name: dd, value: bw2traf(totali[kk],unit)});
			// 				//ddata['traffic_ps'].push({name: dd, value: totali[kk]*8/60});
			// 			} else {
			// 				if(!ddata[tt]) ddata[tt] = [];
			// 				ddata[tt].push({name: dd, value: totali[kk]});
			// 			}
			// 		}
			// 		//	break;
			// 		//case 'speed_request':
			// 		else if(/speed_request/.test(tt)) {
			// 			t1[dd]  = totali[kk];
			// 		}
			// 		//	break;
			// 		//case 'request_count_2xx':
			// 		else if(/request_count_2xx/.test(tt)) {
			// 			t2[dd]  = totali[kk];
			// 		//	break;
			// 		} else {
			// 		//default:
			// 			if(!ddata[tt]) ddata[tt] = [];
			// 			ddata[tt].push({name: dd, value: totali[kk]});
			// 		}
			// 	//}
			// })

			// if(type_avg_speed) {
			// 	var tt2 = [];
			// 	Object.keys(t1).forEach(function(kk){
			// 		var vva = t2[kk] ? t1[kk] / t2[kk] : 0;
			// 		tt2.push({name: kk, value: vva});
			// 	})
			// 	total.push({type: 'avg_speed', value: tt2});
			// }
			cache.set(cache_total_key, JSON.stringify({data: final_result, total: total}), 30*1000);
			res.send({status: true, unit: unit, data: final_result, total: total});
		})
	})

	server.listen(8001, function () {});
