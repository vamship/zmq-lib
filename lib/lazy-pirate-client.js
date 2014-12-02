/* jshint node:true, expr:true */
'use strict';

var _events = require('events');
var _util = require('util');
var _zmq = require('zmq');

var _eventDefinitions = require('./event-definitions');
var Monitor = require('./monitor');
var SocketManager = require('./socket-manager')
var _eventDefinitions = require('./event-definitions');

function _createMessageHandler(instance) {
    return function() {
        var frames = Array.prototype.splice.call(arguments, 0);
        this._lastMessage = null;
        this._monitor.clear();
        this.emit(_eventDefinitions.RESPONSE, frames);
    }.bind(instance);
}

function _createRetryHandler(instance) {
    return function(maxRetriesExceeded) {
        this._dispose();
        if (maxRetriesExceeded) {
            this.emit(_eventDefinitions.ABANDONED, null);
        } else {
            this._initialize(false);
            this._send(this._lastMessage);
        }
    }.bind(instance);
}

/**
 * (Lazy Pirate Client)[http://zguide.zeromq.org/page:all#Client-Side-Reliability-Lazy-Pirate-Pattern]
 * implementation using ZeroMQ on NodeJS. This class will raise the following
 * events:
 *
 * `ready`: Raised when the socket is ready to send data to the client. If a
 *          request was previously sent to the peer, the event handler will
 *          receive the response as the first argument.
 * `abandoned`: Raised when the socket is abandoning retry attempts to the peer.
 *
 * @class LazyPirateClient
 * @constructor
 * @param {String} endpoint The endpoint that the client will connect to.
 * @param {Object} monitor A reference to a preinitialized monitoring object
 *                 that will be used to monitor responses from the peer that
 *                 this socket connects to.
 * @param {Object} [options] Optional ZMQ options object, used to initialize the
 *                 socket.
 */
function LazyPirateClient(endpoint, monitor, options) {
    if (typeof endpoint !== 'string' || endpoint.length <= 0) {
        throw new Error('Invalid endpoint specified (arg #1)');
    }
    if (!(monitor instanceof Monitor)) {
        throw new Error('Invalid retry monitor specified (arg #2)');
    }
    _events.EventEmitter.call(this);

    this._socketManager = new SocketManager('req', endpoint, options);
    this._monitor = monitor;

    this._messageHandler = _createMessageHandler(this);
    this._retry = _createRetryHandler(this);

    this._lastMessage = null;
}

_util.inherits(LazyPirateClient, _events.EventEmitter);

/**
 * @method _initialize
 * @private
 */
LazyPirateClient.prototype._initialize = function(sendEvent) {
    this._socketManager.connectSocket();
    this._socketManager.socket.on('message', this._messageHandler);
    if (sendEvent) {
        this.emit(_eventDefinitions.READY, null);
    }
};

/**
 * @method _dispose
 * @private
 */
LazyPirateClient.prototype._dispose = function() {
    this._socketManager.closeSocket();
};

/**
 * @method _send
 * @private
 */
LazyPirateClient.prototype._send = function(message) {
    this._lastMessage = message;
    this._socketManager.socket.send(message);
    this._monitor.start(this._retry);
};

/**
 * A property that determines whether or not the client is ready to send data
 * to the peer that it is connected to.
 *
 * @method isReady
 * @return {Boolean} True if the socket is ready to send data, false otherwise.
 */
LazyPirateClient.prototype.isReady = function() {
    return (this._socketManager.socket !== null &&
        this._lastMessage === null);
};

/**
 * Initializes the socket, and prepares it to send data to the client. This
 * method must only be invoked for the first time when setting up the initial
 * connection to a peer using a new object.
 *
 * @method initialize
 */
LazyPirateClient.prototype.initialize = function() {
    this._initialize(true);
};

/**
 * Sends a messaage to a connected peer. The client will retry sending the
 * message if no response is received within a specified timeout duration.
 * If a specified number of retries are met with failure, the client will
 * abandon any future attempts at sending data and shut down.
 * The retry duration, and maximum number of retries are both  determined by the
 * monitor object used to create the client.
 *
 * @method send
 * @param {Buffer|String|Array} message The message to be sent to the connected
 *                              peer.
 */
LazyPirateClient.prototype.send = function(message) {
    if (this._socketManager.socket === null) {
        throw new Error('Socket not initialized. Cannot send message');
    }
    if (this._lastMessage !== null) {
        throw new Error('Cannot send message. The socket is still waiting for a response from a previous request');
    }
    this._send(message);
};

/**
 * Disposes the client, and closes any open sockets.
 *
 * @method dispose
 */
LazyPirateClient.prototype.dispose = function() {
    this._dispose();
};

module.exports = LazyPirateClient;
