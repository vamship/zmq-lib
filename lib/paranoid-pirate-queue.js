/* jshint node:true, expr:true */
'use strict';

var _events = require('events');
var _zmq = require('zmq');
var _q = require('q');
var _util = require('util');
var _clone = require('clone');

var Monitor = require('./monitor');
var SocketManager = require('./socket-manager');
var _messageDefinitions = require('./message-definitions');
var _eventDefinitions = require('./event-definitions');

var EMPTY_BUFFER = new Buffer(0);

/**
 * (Paranoid Pirate Queue) [http://zguide.zeromq.org/page:all#Robust-Reliable-Queuing-Paranoid-Pirate-Pattern]
 * implementation that accepts requests from the front end and routes them to
 * the first available worker on the back end. Additionally, the queue also
 * responds to heartbeats from workers, and removes workers from the list
 * of available workers if no heartbeats are received within a specified
 * timeout interval.
 *
 * @class ParanoidPirateQueue
 * @constructor
 * @param {String} feEndpoint The endpoint for the front end of the queue.
 * @param {String} beEndpoint The endpoint for the back end of the queue.
 * @param {Object} monitor A monitor object that will be used to check for 
 *                         dead (or non responsive) workers.
 * @param {Object} [workerOptions] Optional options object that determines how
 *                                 the queue treats workers that miss 
 *                                 heartbeats
 * @param {Object} [feOptions] Optional ZMQ options object, used to initialize
 *                             the front end socket of the queue.
 * @param {Object} [beOptions] Optional ZMQ options object, used to initialize
 *                             the back end socket of the queue.
 */
function ParanoidPirateQueue(feEndpoint, beEndpoint, monitor, workerOptions, feOptions, beOptions) {
    if (typeof feEndpoint !== 'string' || feEndpoint.length <= 0) {
        throw new Error('Invalid front end endpoint specified (arg #1)');
    }
    if (typeof beEndpoint !== 'string' || beEndpoint.length <= 0) {
        throw new Error('Invalid back end endpoint specified (arg #2)');
    }
    if (!(monitor instanceof Monitor)) {
        throw new Error('Invalid monitor specified (arg #3)');
    }

    this._isReady = false;
    this._pendingRequests = [];
    this._availableWorkers = [];
    this._knownWorkers = {};

    var sendToWorker = function(workerAddress, request) {
        request.unshift(EMPTY_BUFFER);
        request.unshift(workerAddress);
        this._backEnd.socket.send(request);
        this.emit(_eventDefinitions.ASSIGNED_REQUEST, workerAddress, request);
    }.bind(this);

    // Initialize front end.
    this._frontEnd = new SocketManager('router', feEndpoint, feOptions);
    this._feMessageHandler = function() {
        var frames = Array.prototype.splice.call(arguments, 0);

        this.emit(_eventDefinitions.REQUEST, frames[0], frames);

        var workerAddress = this._availableWorkers.shift();
        if (workerAddress) {
            sendToWorker(workerAddress, frames);
        } else {
            this._pendingRequests.push(frames);
        }
    }.bind(this);

    // Initialize back end.
    this._backEnd = new SocketManager('router', beEndpoint, beOptions);
    this._beMessageHandler = function() {
        var frames = Array.prototype.splice.call(arguments, 0);

        // Pop out the top two frames (worker address, empty delimiter)
        var workerAddress = frames.shift();
        var empty = frames.shift();

        if (frames.length <= 0) {
            // Bad message from worker. Discard it.
            return;
        }

        if (frames[0].toString() !== _messageDefinitions.READY) {
            this._frontEnd.socket.send(frames);
            this.emit(_eventDefinitions.ASSIGNED_RESPONSE, frames[0], frames);
        }

        var request = this._pendingRequests.shift();
        if (request) {
            sendToWorker(workerAddress, request);
        } else {
            this._availableWorkers.push(workerAddress);
        }
    }.bind(this);
}

_util.inherits(ParanoidPirateQueue, _events.EventEmitter);

/**
 * Initializes the queue, binding the front and back end sockets to their
 * respective endpoints.
 *
 * @method initialize
 */
ParanoidPirateQueue.prototype.initialize = function() {
    var promise = _q.all([
        this._frontEnd.bindSocket(),
        this._backEnd.bindSocket()
    ]);

    promise.then(function success() {
        this._frontEnd.socket.on('message', this._feMessageHandler);
        this._backEnd.socket.on('message', this._beMessageHandler);
        this._isReady = true;
    }.bind(this), function fail() {
        this._isReady = false;
    }.bind(this));

    return promise;
}

/**
 * Disposes the queue, closing both front end and back end sockets.
 *
 * @method dispose
 */
ParanoidPirateQueue.prototype.dispose = function() {
    var promise = _q.all([
        this._frontEnd.closeSocket(),
        this._backEnd.closeSocket()
    ]);

    promise.then(function success() {
        this._isReady = false;
    }.bind(this));

    return promise;
};

/**
 * Retrieves the number of un assigned requests matintained by the queue.
 *
 * @return {Number} The current count of unaasigned requests.
 */
ParanoidPirateQueue.prototype.getPendingRequestCount = function() {
    return this._pendingRequests.length;
};

/**
 * Retrieves the number of free workers available to the queue.
 *
 * @return {Number} The current count of available workers.
 */
ParanoidPirateQueue.prototype.getAvailableWorkerCount = function() {
    return this._availableWorkers.length;
};

/**
 * Returns the list of all workers that the queue is aware of. Note that the
 * returned object is a clone of the original, and will not be updated
 * over time.
 *
 * @return {Object} A hash containing all known workers, along with some
 *                  metadata (last heartbeat, etc.).
 */
ParanoidPirateQueue.prototype.listKnownWorkers = function() {
    return {};
//    return _clone(this._knownWorkers);
};

/**
 * Returns a boolean value that determines whether or not the queue is ready.
 *
 * @return {Boolean} True if the queue is initialized and ready, false
 *                   otherwise.
 */
ParanoidPirateQueue.prototype.isReady = function() {
    return this._isReady;
};


module.exports = ParanoidPirateQueue;
