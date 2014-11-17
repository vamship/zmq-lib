/* jshint node:true, expr:true */
'use strict';

var _zmq = require('zmq');
var _q = require('q');
var SocketManager = require('./socket-manager');
var MessageDefinitions = require('./message-definitions');

var EMPTY_BUFFER = new Buffer(0);

/**
 * Simple queue class that accepts requests from the front end and routes them
 * to the first available worker on the back end.
 *
 * @class SimpleQueue
 * @constructor
 * @param {String} feEndpoint The endpoint for the front end of the queue.
 * @param {String} beEndpoint The endpoint for the back end of the queue.
 * @param {Object} [feOptions] Optional ZMQ options object, used to initialize
 *                 the front end socket of the queue.
 * @param {Object} [beOptions] Optional ZMQ options object, used to initialize
 *                 the back end socket of the queue.
 */
function SimpleQueue(feEndpoint, beEndpoint, feOptions, beOptions) {
    if(typeof feEndpoint !== 'string' || feEndpoint.length <= 0) {
        throw new Error('Invalid front end endpoint specified (arg #1)');
    }
    if(typeof beEndpoint !== 'string' || beEndpoint.length <= 0) {
        throw new Error('Invalid back end endpoint specified (arg #2)');
    }
    this._isReady = false;
    this._pendingRequests = [];
    this._availableWorkers = [];


    var sendToWorker = function(workerAddress, request) {
        request.unshift(EMPTY_BUFFER);
        request.unshift(workerAddress);
        this._backEnd.socket.send(request);
    }.bind(this);

    // Initialize front end.
    this._frontEnd = new SocketManager('router', feEndpoint, feOptions);
    this._feMessageHandler = function() {
        var frames = Array.prototype.splice.call(arguments, 0);
        var workerAddress = this._availableWorkers.shift();
        if(workerAddress) {
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

        if(frames.length <= 0) {
            // Bad message from worker. Discard it.
            return;
        }

        if(frames[0].toString() !== MessageDefinitions.READY) {
            this._frontEnd.socket.send(frames);
        }

        var request = this._pendingRequests.shift();
        if(request) {
            sendToWorker(workerAddress, request);
        } else {
            this._availableWorkers.push(workerAddress);
        }
    }.bind(this);
}

/**
 * Initializes the queue, binding the front and back end sockets to their
 * respective endpoints.
 *
 * @method initialize
 */ 
SimpleQueue.prototype.initialize = function() {
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
};

/**
 * Disposes the queue, closing both front end and back end sockets.
 *
 * @method dispose
 */
SimpleQueue.prototype.dispose = function() {
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
SimpleQueue.prototype.getPendingRequestCount = function() {
    return this._pendingRequests.length;
};

/**
 * Retrieves the number of free workers available to the queue.
 *
 * @return {Number} The current count of available workers.
 */
SimpleQueue.prototype.getAvailableWorkerCount = function() {
    return this._availableWorkers.length;
};

/**
 * Returns a boolean value that determines whether or not the queue is ready.
 *
 * @return {Boolean} True if the queue is initialized and ready, false 
 *                   otherwise.
 */
SimpleQueue.prototype.isReady = function() {
    return this._isReady;
};

module.exports = SimpleQueue;
