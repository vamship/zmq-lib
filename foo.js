var _zmq = require('zmq');



var ep = 'ipc://foo';
var client = _zmq.createSocket('req');
var server = _zmq.createSocket('rep');

server.monitor(10);
server.on('accept', function(){
    console.log('ACCEPT!');
});

server.bind(ep);
client.connect(ep);

setTimeout(function() {
    console.log('Timeout expired!');
    process.exit();
}, 2000);
