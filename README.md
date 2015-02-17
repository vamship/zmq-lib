# zmq-lib
> **NOTE:**: This library has been tested successfully with zeromq version 4.0.5. Testing with older versions of zeromq has revelaed some unstability when executing tests, especially with ugly segmentation faults being thrown as the tests execute.


This is a library of [zero mq](http://zeromq.org/) components that implements some of the patterns described in the [zero mq guide](http://zguide.zeromq.org/page:all). Specifically, the following patterns have been implemented:
* [Lazy Pirate Pattern](http://zguide.zeromq.org/page:all#Client-Side-Reliability-Lazy-Pirate-Pattern): Reliable client that retries requests if no responses are received within a specified timeout.
* [Simple Pirate Queue](http://zguide.zeromq.org/page:all#Basic-Reliable-Queuing-Simple-Pirate-Pattern): A simple queue that routes requests from clients to workers. Load is distributed to workers in the sequence in which they become available. Can be combined with the lazy pirate client and a simple req socket that serves as a worker.
* [Paranoid Pirate Worker/Queue](http://zguide.zeromq.org/page:all#Robust-Reliable-Queuing-Paranoid-Pirate-Pattern): Worker and queue that implement the paranoid pirate pattern. This involves the queue keeping track of active workers, and workers ensuring that the queue is still alive by exchanging heartbeats. This implementation also includes support for a couple of additional features, namely:
    - [Service Brokering](http://zguide.zeromq.org/page:all#Asynchronous-Majordomo-Pattern) (Queue and Worker feature): Allows workers to register for specific services, and to only receive requests that are targeted at those services
    - Session Affinity (Queue Feature): Ensures that all requests from a client are routed to the same worker that received the first request
    - Request Timeouts (Queue Feature): All requests that are not processed within the specified timeout duration will be pruned from the queue
* Lazy Pirate Peer: This is a simple pair socket implementation that provides message retry capabilities. ***NOTE:*** This implementation is still in its early stages, and all of the ramifications of the design have not fully been explored. Treat this as a beta release.

Sample usages of the zmq library can be found [here](https://github.com/vamship/zmq-sample)

In addition to the classes that define the zmq components, the library also exposes additional classes that are useful when using the `zmq-lib`. The following is a quick summary of these classes:
* `MessageDefinitions`: A collection of read only properties that define the different *messages* that are exchanged between zmq sockets.
* `EventDefinitions`: A collection of read only properties that define the different *events* that are emitted by the components in the library.
* `Monitor`: This is a timer implementation that is specifically designed for use within the library's zmq components. More information of how this class is used is specified below.

##Usage
The zmq-lib can be installed using npm:
```
npm install zmq-lib
```
Once installed, `require` the library in your nodejs script by using:
```javascript
var _zmqLib = require('zmq-lib');
```
### Lazy Pirate Client
A lazy pirate client object can be instantiated as follows:
```javascript
var _zmqLib = require('zmq-lib');
var LazyPirateClient = _zmqLib.LazyPirateClient;
var Monitor = _zmqLib.Monitor;
var EventDefinitions = _zmqLib.EventDefinitions;

// Create a monitor that polls every 500 seconds for responses, and will
// attempt retries a maximum of 3 times.
var retryMonitor = new Monitor(500, 3);
var client = new LazyPirateClient('tcp://127.0.0.1:5555', retryMonitor);
```
Once instantiated, the client must be initialized prior to first use. This process makes a connection to a remote socket, and the client will emit the `EventDefinitions.READY` event once initialization is complete.
```javascript
// Create a connection to the remote socket.
client.initialize();
client.on(EventDefinitions.READY, function() {
    console.log('lazy pirate client is ready');
});
```
Messages to the remote socket can be sent using the `send()` method. The payload can be a string, or a buffer, or an array of strings. The client will automatically retry sending the message if no response is received within the retry monitors poll frequency. The retries are handled internally, and are not visible to the caller.
```javascript
// Send a "hello" message to the remote peer.
client.send('hello');
```
>Note that the client is a `req` socket, which means that additional messages cannot be sent over the socket until a response has been received for a previously sent message.

The client will emit events when a response is received from the remote peer.
```javascript
// Handle responses from the remote socket.
client.on(EventDefinitions.RESPONSE, function() {
    var frames = Array.prototype.splice.call(arguments, 0);
    console.log('client received response: ');
    frames.forEach(function(frame) {
        console.log(frame.toString());
    });
});
```

If the client does not receive any responses from the remote sockets after multiple attempts, it will abandon all further attempts and shut down. Under these circumstances, the `EventDefinitions.ABANDONED` event will be emitted.
```javascript
// Handle the abandoned event
client.on(EventDefinitions.ABANDONED, function() {
    console.log('client was unable to communicate with the remote socket');
});
```

### Simple Queue
A simple queue object can be instantiated as follows:
```javascript
var _zmqLib = require('zmq-lib');
var SimpleQueue = _zmqLib.SimpleQueue;
var EventDefinitions = _zmqLib.EventDefinitions;

var queue = new SimpleQueue('tcp://127.0.0.1:5555', //Front end endpoint
                            'tcp://127.0.0.1:5556' //Back end endpoint);
```
Once instantiated, the queue must be initialized prior to first use. This process binds the frontend and backend endpoints. The function call returns a promise that will be fulfilled or rejected based on whether or not the endpoints were properly initialized.
```javascript
// Initialize the queue endpoints
queue.initialize().then(function() {
    console.log('Queue initialized successfully');
}, function(err) {
    console.log('An error occurred when initializing the queue', err);
});
```
At this point, the queue is ready to receive requests from clients and workers on the frontend and backend sockets respectively. While no further action is required, the queue does emit events that can be used for monitoring/tracking purposes.
* `EventDefinitions.REQUEST`: This event is emitted when a new request is received from a client.
* `EventDefinitions.ASSIGNED_REQUEST`: This event is emitted when a request is assigned to a worker.
* `EventDefinitions.ASSIGNED_RESPONSE`: This event is emitted when a response from a worker is assigned back to a client.

```javascript
queue.on(EventDefinitions.REQUEST, function(clientId, frames) {
    // Note that the data frames will be the raw request, including addressing
    // information.
    console.log('Request received from client:', clientId);
    frames.forEach(function(frame) {
        console.log(frame.toString());
    });
});

queue.on(EventDefinitions.ASSIGNED_REQUEST, function(workerAddress, frames) {
    // Note that the data frames will be the raw request, including addressing
    // information.
    console.log('Request assigned to worker:', workerAddress);
    frames.forEach(function(frame) {
        console.log(frame.toString());
    });
});

queue.on(EventDefinitions.ASSIGNED_RESPONSE, function(clientAddress, frames) {
    // Note that the data frames will be the raw request, including addressing
    // information.
    console.log('Response assigned to client:', clientAddress);
    frames.forEach(function(frame) {
        console.log(frame.toString());
    });
});
```

### Paranoid Pirate Queue
A paranoid pirate queue object can be instantiated as follows:
```javascript
var _zmqLib = require('zmq-lib');
var ParanoidPirateQueue = _zmqLib.ParanoidPirateQueue;
var EventDefinitions = _zmqLib.EventDefinitions;

var queueOptions = {
    // The frequency with which the queue will check for dead workers
    pollFrequency: 1000,
    
    // A timeout period that defines how long a worker can go without any
    // communication with the queue before it is considered dead.
    workerTimeout: 3000,

    // A timeout period that defines how long a request can remain
    // unprocessed in the queue before it is considered dead and pruned.
    requestTimeout: 6000,

    // This object is optional, and if omitted, will disable session affinity
    // on the queue. Use with care.
    session: {

        // The duration (in milliseconds) after which the session will be 
        // discarded.
        timeout: 30000
    }
};

var queue = new ParanoidPirateQueue('tcp://127.0.0.1:5555', //Front end endpoint
                                    'tcp://127.0.0.1:5556', //Back end endpoint
                                    queueOptions);
```
Once instantiated, the queue must be initialized prior to first use. This process binds the frontend and backend endpoints. The function call returns a promise that will be fulfilled or rejected based on whether or not the endpoints were properly initialized.
```javascript
// Initialize the queue endpoints
queue.initialize().then(function() {
    console.log('Queue initialized successfully');
}, function(err) {
    console.log('An error occurred when initializing the queue', err);
});
```
At this point, the queue is ready to receive requests from clients and workers on the frontend and backend sockets respectively. While no further action is required, the queue does emit events that can be used for monitoring/tracking purposes.
* `EventDefinitions.REQUEST`: This event is emitted when a new request is received from a client.
* `EventDefinitions.ASSIGNED_REQUEST`: This event is emitted when a request is assigned to a worker.
* `EventDefinitions.ASSIGNED_RESPONSE`: This event is emitted when a response from a worker is assigned back to a client.

```javascript
queue.on(EventDefinitions.REQUEST, function(clientId, frames) {
    // Note that the data frames will be the raw request, including addressing
    // information.
    console.log('Request received from client:', clientId);
    frames.forEach(function(frame) {
        console.log(frame.toString());
    });
});

queue.on(EventDefinitions.ASSIGNED_REQUEST, function(workerAddress, frames) {
    // Note that the data frames will be the raw request, including addressing
    // information.
    console.log('Request assigned to worker:', workerAddress);
    frames.forEach(function(frame) {
        console.log(frame.toString());
    });
});

queue.on(EventDefinitions.ASSIGNED_RESPONSE, function(clientAddress, frames) {
    // Note that the data frames will be the raw request, including addressing
    // information.
    console.log('Response assigned to client:', clientAddress);
    frames.forEach(function(frame) {
        console.log(frame.toString());
    });
});
```

### Paranoid Pirate Worker
A paranoid pirate worker object can be instantiated as follows:
```javascript
var _zmqLib = require('zmq-lib');
var ParanoidPirateWorker = _zmqLib.ParanoidPirateWorker;
var Monitor = _zmqLib.Monitor;
var EventDefinitions = _zmqLib.EventDefinitions;

// Create a monitor that sends out heartbeats at regular intervals, and
// triggers a retry once the specified number of iterations have passed
// without any communication from the remote sockets.
var retryMonitor = new Monitor(500, 3);

// Create an object that represents worker options.
var workerOptions = {

    // The number of milliseconds to wait before attempting to reconnect to
    // the remote sockets when heartbeat failure is detected. This number will
    // be doubled with each attempt.
    backoff: 100,

    // The number of times to reattempt connection with the remote socket before
    // abandoning attempts.
    retries: 3,

    // The number of milliseconds that can elapse without getting any requests
    // from the peer, after which the worker will terminate. This timeout is
    // specific to requests, and will apply even if heartbeats are being 
    // received normally. The intent behind this parameter is to allow the
    // worker to detect low load scenarios (very few client requests coming in
    // and a lot of workers available to process the load), and unilaterally
    // terminate.

    // This is an optional parameter, and if omitted, worker will never
    // terminate when requests are not received.
    idleTimeout: 10000
};

var worker = new ParanoidPirateWorker('tcp://127.0.0.1:5555', retryMonitor, workerOptions);
```
Once instantiated, the worker must be initialized prior to first use. This process makes a connection to a remote socket, and the worker will emit the `EventDefinitions.READY` event once initialization is complete.
```javascript
// Create a connection to the remote socket.
worker.initialize();
worker.on(EventDefinitions.READY, function() {
    console.log('paranoid pirate worker is ready');
});
```
Messages to the remote socket can be sent using the `send()` method. The payload can be a string, or a buffer, or an array of strings.

```javascript
// Send a "hello" message to the remote peer.
worker.send('hello');
```
>Note that the worker is a `dealer` socket, which means that `send()` can be invoked multiple times when responding to requests. An optional second parameter to the `send()` method can be used to tell the worker that more messages are being sent as a part of the response. If omitted, this parameter will default to `false`, and the response will be deemed complete.

```javascript
// Send the first message in the response.
worker.send('first', true);

// Send the second message in the response.
worker.send('second', true);

// Send the final message in the response.
worker.send('final', false);
```

The worker will emit events when a request is received from the remote peer.
```javascript
// Handle responses from the remote socket.
worker.on(EventDefinitions.REQUEST, function() {
    var frames = Array.prototype.splice.call(arguments, 0);
    console.log('worker received request: ');
    frames.forEach(function(frame) {
        console.log(frame.toString());
    });
});
```

If the worker does not receive any heartbeats from the remote sockets after multiple attempts, or if the request timeout expires (if applicable), it will abandon all further attempts and shut down. Under these circumstances, the `EventDefinitions.ABANDONED` event will be emitted.
```javascript
// Handle the abandoned event
worker.on(EventDefinitions.ABANDONED, function(reason) {
    console.log('Worker is shutting down. Reason:', reason);
});
```


### Gruntfile
This project template is provided with a Gruntfile that contains task definitions for most common development activities. This includes - linting, testing and bumping version numbers. It is strongly recommended that the tasks defined in the Gruntfile be leveraged to the maximum extent possible. More information can be obtained by typing:
```
grunt help
```
# zmq-lib
> **NOTE:** This library has been tested successfully with zeromq version 4.0.5. Testing with older versions of zeromq has revelaed some unstability when executing tests, especially with ugly segmentation faults being thrown as the tests execute.

This is a library of [zero mq](http://zeromq.org/) components that implements some of the patterns described in the [zero mq guide](http://zguide.zeromq.org/page:all). Specifically, the following patterns have been implemented:

* [Lazy Pirate Pattern](http://zguide.zeromq.org/page:all#Client-Side-Reliability-Lazy-Pirate-Pattern): Reliable client that retries requests if no responses are received within a specified timeout.
* [Simple Pirate Queue](http://zguide.zeromq.org/page:all#Basic-Reliable-Queuing-Simple-Pirate-Pattern): A simple queue that routes requests from clients to workers. Load is distributed to workers in the sequence in which they become available. Can be combined with the lazy pirate client and a simple req socket that serves as a worker.
* [Paranoid Pirate Worker/Queue](http://zguide.zeromq.org/page:all#Robust-Reliable-Queuing-Paranoid-Pirate-Pattern): Worker and queue that implement the paranoid pirate pattern. This involves the queue keeping track of active workers, and workers ensuring that the queue is still alive by exchanging heartbeats. This implementation also includes support for a couple of additional features, namely:
    - [Service Brokering](http://zguide.zeromq.org/page:all#Asynchronous-Majordomo-Pattern) (Queue and Worker feature): Allows workers to register for specific services, and to only receive requests that are targeted at those services
    - Session Affinity (Queue Feature): Ensures that all requests from a client are routed to the same worker that received the first request
    - Request Timeouts (Queue Feature): All requests that are not processed within the specified timeout duration will be pruned from the queue
* Lazy Pirate Peer: This is a simple pair socket implementation that provides message retry capabilities. ***NOTE:*** This implementation is still in its early stages, and all of the ramifications of the design have not fully been explored. Treat this as a beta release.

Sample usages of the zmq library can be found [here](https://github.com/vamship/zmq-sample)

In addition to the classes that define the zmq components, the library also exposes additional classes that are useful when using the `zmq-lib`. The following is a quick summary of these classes:
* `MessageDefinitions`: A collection of read only properties that define the different *messages* that are exchanged between zmq sockets.
* `EventDefinitions`: A collection of read only properties that define the different *events* that are emitted by the components in the library.
* `Monitor`: This is a timer implementation that is specifically designed for use within the library's zmq components. More information of how this class is used is specified below.

##Usage
The zmq-lib can be installed using npm:
```
npm install zmq-lib
```
Once installed, `require` the library in your nodejs script by using:
```javascript
var _zmqLib = require('zmq-lib');
```
### Lazy Pirate Client
A lazy pirate client object can be instantiated as follows:
```javascript
var _zmqLib = require('zmq-lib');
var LazyPirateClient = _zmqLib.LazyPirateClient;
var Monitor = _zmqLib.Monitor;
var EventDefinitions = _zmqLib.EventDefinitions;

// Create a monitor that polls every 500 seconds for responses, and will
// attempt retries a maximum of 3 times.
var retryMonitor = new Monitor(500, 3);
var client = new LazyPirateClient('tcp://127.0.0.1:5555', retryMonitor);
```
Once instantiated, the client must be initialized prior to first use. This process makes a connection to a remote socket, and the client will emit the `EventDefinitions.READY` event once initialization is complete.
```javascript
// Create a connection to the remote socket.
client.initialize();
client.on(EventDefinitions.READY, function() {
    console.log('lazy pirate client is ready');
});
```
Messages to the remote socket can be sent using the `send()` method. The payload can be a string, or a buffer, or an array of strings. The client will automatically retry sending the message if no response is received within the retry monitors poll frequency. The retries are handled internally, and are not visible to the caller.
```javascript
// Send a "hello" message to the remote peer.
client.send('hello');
```
>Note that the client is a `req` socket, which means that additional messages cannot be sent over the socket until a response has been received for a previously sent message.

The client will emit events when a response is received from the remote peer.
```javascript
// Handle responses from the remote socket.
client.on(EventDefinitions.RESPONSE, function() {
    var frames = Array.prototype.splice.call(arguments, 0);
    console.log('client received response: ');
    frames.forEach(function(frame) {
        console.log(frame.toString());
    });
});
```

If the client does not receive any responses from the remote sockets after multiple attempts, it will abandon all further attempts and shut down. Under these circumstances, the `EventDefinitions.ABANDONED` event will be emitted.
```javascript
// Handle the abandoned event
client.on(EventDefinitions.ABANDONED, function() {
    console.log('client was unable to communicate with the remote socket');
});
```

### Simple Queue
A simple queue object can be instantiated as follows:
```javascript
var _zmqLib = require('zmq-lib');
var SimpleQueue = _zmqLib.SimpleQueue;
var EventDefinitions = _zmqLib.EventDefinitions;

var queue = new SimpleQueue('tcp://127.0.0.1:5555', //Front end endpoint
                            'tcp://127.0.0.1:5556' //Back end endpoint);
```
Once instantiated, the queue must be initialized prior to first use. This process binds the frontend and backend endpoints. The function call returns a promise that will be fulfilled or rejected based on whether or not the endpoints were properly initialized.
```javascript
// Initialize the queue endpoints
queue.initialize().then(function() {
    console.log('Queue initialized successfully');
}, function(err) {
    console.log('An error occurred when initializing the queue', err);
});
```
At this point, the queue is ready to receive requests from clients and workers on the frontend and backend sockets respectively. While no further action is required, the queue does emit events that can be used for monitoring/tracking purposes.
* `EventDefinitions.REQUEST`: This event is emitted when a new request is received from a client.
* `EventDefinitions.ASSIGNED_REQUEST`: This event is emitted when a request is assigned to a worker.
* `EventDefinitions.ASSIGNED_RESPONSE`: This event is emitted when a response from a worker is assigned back to a client.

```javascript
queue.on(EventDefinitions.REQUEST, function(clientId, frames) {
    // Note that the data frames will be the raw request, including addressing
    // information.
    console.log('Request received from client:', clientId);
    frames.forEach(function(frame) {
        console.log(frame.toString());
    });
});

queue.on(EventDefinitions.ASSIGNED_REQUEST, function(workerAddress, frames) {
    // Note that the data frames will be the raw request, including addressing
    // information.
    console.log('Request assigned to worker:', workerAddress);
    frames.forEach(function(frame) {
        console.log(frame.toString());
    });
});

queue.on(EventDefinitions.ASSIGNED_RESPONSE, function(clientAddress, frames) {
    // Note that the data frames will be the raw request, including addressing
    // information.
    console.log('Response assigned to client:', clientAddress);
    frames.forEach(function(frame) {
        console.log(frame.toString());
    });
});
```

### Paranoid Pirate Queue
A paranoid pirate queue object can be instantiated as follows:
```javascript
var _zmqLib = require('zmq-lib');
var ParanoidPirateQueue = _zmqLib.ParanoidPirateQueue;
var EventDefinitions = _zmqLib.EventDefinitions;

var queueOptions = {
    // The frequency with which the queue will check for dead workers
    pollFrequency: 1000,
    
    // A timeout period that defines how long a worker can go without any
    // communication with the queue before it is considered dead.
    workerTimeout: 3000,

    // A timeout period that defines how long a request can remain
    // unprocessed in the queue before it is considered dead and pruned.
    requestTimeout: 6000,

    // This object is optional, and if omitted, will disable session affinity
    // on the queue. Use with care.
    session: {

        // The duration (in milliseconds) after which the session will be 
        // discarded.
        timeout: 30000
    }
};

var queue = new ParanoidPirateQueue('tcp://127.0.0.1:5555', //Front end endpoint
                                    'tcp://127.0.0.1:5556', //Back end endpoint
                                    queueOptions);
```
Once instantiated, the queue must be initialized prior to first use. This process binds the frontend and backend endpoints. The function call returns a promise that will be fulfilled or rejected based on whether or not the endpoints were properly initialized.
```javascript
// Initialize the queue endpoints
queue.initialize().then(function() {
    console.log('Queue initialized successfully');
}, function(err) {
    console.log('An error occurred when initializing the queue', err);
});
```
At this point, the queue is ready to receive requests from clients and workers on the frontend and backend sockets respectively. While no further action is required, the queue does emit events that can be used for monitoring/tracking purposes.
* `EventDefinitions.REQUEST`: This event is emitted when a new request is received from a client.
* `EventDefinitions.ASSIGNED_REQUEST`: This event is emitted when a request is assigned to a worker.
* `EventDefinitions.ASSIGNED_RESPONSE`: This event is emitted when a response from a worker is assigned back to a client.

```javascript
queue.on(EventDefinitions.REQUEST, function(clientId, frames) {
    // Note that the data frames will be the raw request, including addressing
    // information.
    console.log('Request received from client:', clientId);
    frames.forEach(function(frame) {
        console.log(frame.toString());
    });
});

queue.on(EventDefinitions.ASSIGNED_REQUEST, function(workerAddress, frames) {
    // Note that the data frames will be the raw request, including addressing
    // information.
    console.log('Request assigned to worker:', workerAddress);
    frames.forEach(function(frame) {
        console.log(frame.toString());
    });
});

queue.on(EventDefinitions.ASSIGNED_RESPONSE, function(clientAddress, frames) {
    // Note that the data frames will be the raw request, including addressing
    // information.
    console.log('Response assigned to client:', clientAddress);
    frames.forEach(function(frame) {
        console.log(frame.toString());
    });
});
```

### Paranoid Pirate Worker
A paranoid pirate worker object can be instantiated as follows:
```javascript
var _zmqLib = require('zmq-lib');
var ParanoidPirateWorker = _zmqLib.ParanoidPirateWorker;
var Monitor = _zmqLib.Monitor;
var EventDefinitions = _zmqLib.EventDefinitions;

// Create a monitor that sends out heartbeats at regular intervals, and
// triggers a retry once the specified number of iterations have passed
// without any communication from the remote sockets.
var retryMonitor = new Monitor(500, 3);

// Create an object that represents worker options.
var workerOptions = {

    // The number of milliseconds to wait before attempting to reconnect to
    // the remote sockets when heartbeat failure is detected. This number will
    // be doubled with each attempt.
    backoff: 100,

    // The number of times to reattempt connection with the remote socket before
    // abandoning attempts.
    retries: 3,

    // The number of milliseconds that can elapse without getting any requests
    // from the peer, after which the worker will terminate. This timeout is
    // specific to requests, and will apply even if heartbeats are being 
    // received normally. The intent behind this parameter is to allow the
    // worker to detect low load scenarios (very few client requests coming in
    // and a lot of workers available to process the load), and unilaterally
    // terminate.

    // This is an optional parameter, and if omitted, worker will never
    // terminate when requests are not received.
    idleTimeout: 10000
};

var worker = new ParanoidPirateWorker('tcp://127.0.0.1:5555', retryMonitor, workerOptions);
```
Once instantiated, the worker must be initialized prior to first use. This process makes a connection to a remote socket, and the worker will emit the `EventDefinitions.READY` event once initialization is complete.
```javascript
// Create a connection to the remote socket.
worker.initialize();
worker.on(EventDefinitions.READY, function() {
    console.log('paranoid pirate worker is ready');
});
```
Messages to the remote socket can be sent using the `send()` method. The payload can be a string, or a buffer, or an array of strings.

```javascript
// Send a "hello" message to the remote peer.
worker.send('hello');
```
>Note that the worker is a `dealer` socket, which means that `send()` can be invoked multiple times when responding to requests. An optional second parameter to the `send()` method can be used to tell the worker that more messages are being sent as a part of the response. If omitted, this parameter will default to `false`, and the response will be deemed complete.

```javascript
// Send the first message in the response.
worker.send('first', true);

// Send the second message in the response.
worker.send('second', true);

// Send the final message in the response.
worker.send('final', false);
```

The worker will emit events when a request is received from the remote peer.
```javascript
// Handle responses from the remote socket.
worker.on(EventDefinitions.REQUEST, function() {
    var frames = Array.prototype.splice.call(arguments, 0);
    console.log('worker received request: ');
    frames.forEach(function(frame) {
        console.log(frame.toString());
    });
});
```

If the worker does not receive any heartbeats from the remote sockets after multiple attempts, or if the request timeout expires (if applicable), it will abandon all further attempts and shut down. Under these circumstances, the `EventDefinitions.ABANDONED` event will be emitted.
```javascript
// Handle the abandoned event
worker.on(EventDefinitions.ABANDONED, function(reason) {
    console.log('Worker is shutting down. Reason:', reason);
});
```


### Gruntfile
This project template is provided with a Gruntfile that contains task definitions for most common development activities. This includes - linting, testing and bumping version numbers. It is strongly recommended that the tasks defined in the Gruntfile be leveraged to the maximum extent possible. More information can be obtained by typing:
```
grunt help
```
