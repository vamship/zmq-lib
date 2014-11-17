/* jshint node:true, expr:true */
'use strict';

var _util = require('util');
var _events = require('events');
var _zmq = require('zmq');
var _q = require('q');
var SocketManager = require('./socket-manager');
var Monitor = require('./monitor');
var MessageDefinitions = require('./message-definitions');

/**
 * Worker class that implements the paranoid pirate pattern. Connects to a
 * queue, monitors the connection, and restarts the connection if it 
 * detects that the queue has died, or has stopped responding. Connection
 * restart is performed with exponential backoff.
 *
 * @class ParanoidPirateWorker
 * @constructor
 * @param {String} endpoint The endpoint that the worker will connect to.
 * @param {Object} monitor A reference to a preinitialized monitoring object
 *                 that will be used to monitor connections to the peer that
 *                 this socket connects to.
 * @param {Object} [options] Optional ZMQ options object, used to initialize the
 *                 socket.
 */
function ParanoidPirateWorker(endpoint, monitor, options) {
    if(typeof endpoint !== 'string' || endpoint.length <=0){
        throw new Error('Invalid endpoint specified (arg #1)');
    }
    if(!(monitor instanceof Monitor)) {
        throw new Error('Invalid retry monitor specified (arg #2)');
    }
    _events.EventEmitter.call(this);

    this._socketManager = new SocketManager('dealer', endpoint, options);
}

_util.inherits(ParanoidPirateWorker, _events.EventEmitter);

/**
 * @method _initialize
 * @private
 */
ParanoidPirateWorker.prototype._initialize = function(sendEvent) {
    this._socketManager.connectSocket();
//    this._socketManager.socket.on('message', this._messageHandler);
    this._socketManager.socket.send(['', MessageDefinitions.READY]);
    if(sendEvent) {
        this.emit('ready', null);
    }
};

/**
 * @method _dispose
 * @private
 */
ParanoidPirateWorker.prototype._dispose = function() {
//    this._socketManager.closeSocket();
};

/**
 * @method _send
 * @private
 */
ParanoidPirateWorker.prototype._send = function(message) {
//    this._lastMessage = message;
//    this._socketManager.socket.send(message);
//    this._monitor.start(this._retry);
};

/**
 * A property that determines whether or not the client is ready to send data
 * to the peer that it is connected to.
 *
 * @method isReady
 * @return {Boolean} True if the socket is ready to send data, false otherwise.
 */
ParanoidPirateWorker.prototype.isReady = function() {
    return false;
//    return (this._socketManager.socket !== null &&
//            this._lastMessage === null);
};

/**
 * Initializes the socket, and prepares it to send data to the client. This
 * method must only be invoked for the first time when setting up the initial
 * connection to a peer using a new object.
 *
 * @method initialize
 */
ParanoidPirateWorker.prototype.initialize = function() {
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
ParanoidPirateWorker.prototype.send = function(message) {
//    if(this._socketManager.socket === null) {
//        throw new Error( 'Socket not initialized. Cannot send message');
//    }
//    if(this._lastMessage !== null) {
//        throw new Error('Cannot send message. The socket is still waiting for a response from a previous request');
//    }
//    this._send(message);
};

/**
 * Disposes the client, and closes any open sockets.
 *
 * @method dispose
 */
ParanoidPirateWorker.prototype.dispose = function() {
//    this._dispose();
};

module.exports = ParanoidPirateWorker;
