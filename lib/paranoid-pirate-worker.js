/* jshint node:true, expr:true */
'use strict';

var _util = require('util');
var _events = require('events');
var _zmq = require('zmq');
var _q = require('q');

var _messageDefinitions = require('./message-definitions');
var _eventDefinitions = require('./event-definitions');
var Monitor = require('./monitor');
var SocketManager = require('./socket-manager');

var DEFAULT_WORKER_OPTIONS = {
    backoff: 100,
    retries: 10,
    idleTimeout: -1
};

function _createMessageHandler(instance) {
    return function() {
        var frames = Array.prototype.splice.call(arguments, 0);
        var action = frames[0].toString();
        if (action === _messageDefinitions.HEARTBEAT) {
            this._backoffRetries = this._backoffOptions.retries;
            this._backoff = this._backoffOptions.backoff;
            this._monitor.resetExpiryCount();
        } else {
            this._workingOnRequest = true;
            this.emit(_eventDefinitions.REQUEST, frames);
        }
    }.bind(instance);
}

function _createHeartBeater(instance) {
    return function(maxRetriesExceeded) {
        var curTime = Date.now();

        if (!this._workingOnRequest && this._idleTimeout > 0 &&
            curTime - this._lastRequestTimestamp > this._idleTimeout) {
            this.emit(_eventDefinitions.ABANDONED, 'idle timeout expired');
        } else if (maxRetriesExceeded) {
            this._socketManager.closeSocket();
            this._monitor.clear();


            if (this._backoffRetries > 0) {
                setTimeout(function() {
                    this._initialize(true);
                }.bind(this), this._backoff);

                this._backoff = this._backoff * 2;
                this._backoffRetries--;
            } else {
                this.emit(_eventDefinitions.ABANDONED, 'peer unreachable');
            }

        } else {
            this._send(_messageDefinitions.HEARTBEAT);
            this._monitor.start(this._heartBeater);
        }
    }.bind(instance);
}

/**
 * (Paranoid Pirate Worker) [http://zguide.zeromq.org/page:all#Robust-Reliable-Queuing-Paranoid-Pirate-Pattern]
 * implementation. Connects to a peer, monitors the connection, and restarts
 * the connection if it detects that the queue has died, or has stopped
 * responding. Connection restart is performed with exponential backoff.
 *
 * @class ParanoidPirateWorker
 * @constructor
 * @param {String} endpoint The endpoint that the worker will connect to.
 * @param {Object} monitor A reference to a preinitialized monitoring object
 *                 that will be used to monitor connections to the peer that
 *                 this socket connects to.
 * @param {Object} [workerOptions={backoff: 100, retries: 10}] Optional
 *                 worker options that can be used to describe backoff/retry
 *                 and idle timeout termination behavior.
 * @param {Object} [socketOptions] Optional ZMQ options object, used to initialize the
 *                 socket.
 */
function ParanoidPirateWorker(endpoint, monitor, workerOptions, socketOptions) {
    if (typeof endpoint !== 'string' || endpoint.length <= 0) {
        throw new Error('Invalid endpoint specified (arg #1)');
    }
    if (!(monitor instanceof Monitor)) {
        throw new Error('Invalid monitor specified (arg #2)');
    }
    _events.EventEmitter.call(this);

    this._socketManager = new SocketManager('dealer', endpoint, socketOptions);
    this._monitor = monitor;
    workerOptions = workerOptions || {};
    this._backoffOptions = {
        backoff: workerOptions.backoff || DEFAULT_WORKER_OPTIONS.backoff,
        retries: workerOptions.retries || DEFAULT_WORKER_OPTIONS.retries
    };
    this._idleTimeout = workerOptions.idleTimeout || DEFAULT_WORKER_OPTIONS.idleTimeout;

    this._backoffRetries = this._backoffOptions.retries;
    this._backoff = this._backoffOptions.backoff;
    this._workingOnRequest = false;
    this._lastRequestTimestamp = Date.now();

    this._messageHandler = _createMessageHandler(this);
    this._heartBeater = _createHeartBeater(this);
    this._services = [];
}

_util.inherits(ParanoidPirateWorker, _events.EventEmitter);

/**
 * @method _initialize
 * @private
 */
ParanoidPirateWorker.prototype._initialize = function(emitEvent) {
    this._socketManager.connectSocket();
    this._socketManager.socket.on('message', this._messageHandler);
    this._socketManager.socket.send([_messageDefinitions.READY].concat(this._services));
    if (emitEvent) {
        this.emit(_eventDefinitions.READY, null);
    }
    this._monitor.start(this._heartBeater);
};

/**
 * @method _dispose
 * @private
 */
ParanoidPirateWorker.prototype._dispose = function() {
    this._monitor.clear();
    this._socketManager.closeSocket();
};

/**
 * @method _send
 * @private
 */
ParanoidPirateWorker.prototype._send = function(message) {
    if (!message instanceof Array) {
        message = [message];
    }

    this._socketManager.socket.send(message);
};

/**
 * A property that determines whether or not the client is ready to send data
 * to the peer that it is connected to.
 *
 * @method isReady
 * @return {Boolean} True if the socket is ready to send data, false otherwise.
 */
ParanoidPirateWorker.prototype.isReady = function() {
    return (this._socketManager.socket !== null);
};

/**
 * Initializes the socket, and prepares it to send data to the peer. This
 * method must only be invoked for the first time when setting up the initial
 * connection to a peer using a new object.
 *
 * @method initialize
 * @param {...String} [services] Optional service names to use when registering
 *                    the worker.
 */
ParanoidPirateWorker.prototype.initialize = function() {
    var services = Array.prototype.splice.call(arguments, 0);
    this._services = services;
    this._initialize(true);
};

/**
 * Sends a messaage to a connected peer. The worker will send periodic
 * heartbeats to the peer, and will deduce failure of the peer if a specific
 * number of heartbeats pass with no response. In this scenario, the worker
 * will close the socket, reopen it and attempt to reconnect.
 * The heartbeat frequency, and maximum number of retries are both determined by the
 * monitor object used to create the client.
 *
 * @method send
 * @param {Buffer|String|Array} message The message to be sent to the connected
 *                              peer.
 * @param {Boolean} [notDone=false] An optional parameter that indicates that
 *                  the worker is not done processing the request.
 */
ParanoidPirateWorker.prototype.send = function(message, notDone) {
    if (this._socketManager.socket === null) {
        throw new Error('Socket not initialized. Cannot send message');
    }
    this._workingOnRequest = !!notDone;
    this._send(message);
};

/**
 * Disposes the client, and closes any open sockets.
 *
 * @method dispose
 */
ParanoidPirateWorker.prototype.dispose = function() {
    this._dispose();
};

/**
 * Returns a copy of the backoff options used by the socket. These options
 * govern the retry behavior of the socket when it detects a dead peer.
 *
 * @method getBackoffOptions
 * @return {Object} An object containing the backoff options for the socket.
 */
ParanoidPirateWorker.prototype.getBackoffOptions = function() {
    return {
        backoff: this._backoffOptions.backoff,
        retries: this._backoffOptions.retries
    };
};

module.exports = ParanoidPirateWorker;
