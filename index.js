var cluster = require('cluster'),
//    numCPUs = require('os').cpus().length;
    numCPUs = 1;

if (cluster.isMaster) {
    while (numCPUs-- > 0) cluster.fork();

    cluster.on('exit', function(worker, code, signal) {
        console.log('worker ' + worker.process.pid + ' died');
        cluster.fork();
    });
} else {
    require('./app_v1.6.js');
}
