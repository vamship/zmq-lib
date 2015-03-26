/* jshint node:true, expr:true */
'use strict';

var _events = require('events');
var _zmq = require('zmq');
var _q = require('q');
var _util = require('util');
var SocketManager = require('./socket-manager');
var _messageDefinitions = require('./message-definitions');
var _eventDefinitions = require('./event-definitions');

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
    if (typeof feEndpoint !== 'string' || feEndpoint.length <= 0) {
        throw new Error('Invalid front end endpoint specified (arg #1)');
    }
    if (typeof beEndpoint !== 'string' || beEndpoint.length <= 0) {
        throw new Error('Invalid back end endpoint specified (arg #2)');
    }
    this._isReady = false;
    this._pendingRequests = [];
    this._availableWorkers = [];

    // Initialize front end.
    this._frontEnd = new SocketManager('router', feEndpoint, feOptions);
    this._feMessageHandler = this._getFrontEndMessageHandler();

    // Initialize back end.
    this._backEnd = new SocketManager('router', beEndpoint, beOptions);
    this._beMessageHandler = this._getBackEndMessageHandler();
}

_util.inherits(SimpleQueue, _events.EventEmitter);

/**
 * Sends a message to a specific worker. Can be overridden by inheriting objects
 * to provide different implementations (different protocols).
 *
 * @method _sendToWorker
 * @protected
 * @param {Object} workerAddress A buffer that represents the address of the worker
 *                               entitiy.
 * @param {Array} data An array of buffers that represents the data payload to be
 *                     sent to the worker.
 */
SimpleQueue.prototype._sendToWorker = function(workerAddress, request) {
    request.unshift(EMPTY_BUFFER);
    request.unshift(workerAddress);
    this._backEnd.socket.send(request);
    this.emit(_eventDefinitions.ASSIGNED_REQUEST, workerAddress, request);
};

/**
 * Returns a handler for messages from workers.
 *
 * @method _getBackEndMessageHandler
 * @protected
 * @return {Function} A handler that will receive and process messages from the
 *                    backend (i.e., workers).
 */
SimpleQueue.prototype._getBackEndMessageHandler = function() {
    return function() {
        var frames = Array.prototype.splice.call(arguments, 0);

        // Pop out the top two frames (worker address, empty delimiter)
        var workerAddress = frames.shift();
        var empty = frames.shift();

        if (frames.length <= 0) {
            // Bad message from worker. Discard it.

            // Note: This may actually never happen. It seems impossible
            // to send **nothing** over zmq. By "nothing", I am referring
            // not to an empty frame, but actually no frames.
            // So, it may not be able to send a message with no frames,
            // and this check is possibly redundant.
            return;
        }

        if (frames[0].toString() !== _messageDefinitions.READY) {
            this._frontEnd.socket.send(frames);
            this.emit(_eventDefinitions.ASSIGNED_RESPONSE, frames[0], frames);
        }

        var request = this._pendingRequests.shift();
        if (request) {
            this._sendToWorker(workerAddress, request);
        } else {
            this._availableWorkers.push(workerAddress);
        }
    }.bind(this);
};

/**
 * Returns a handler for messages from clients.
 *
 * @method _getFrontEndMessageHandler
 * @protected
 * @return {Function} A handler that will receive and process messages from the
 *                    frontend (i.e., clients).
 */
SimpleQueue.prototype._getFrontEndMessageHandler = function() {
    return function() {
        var frames = Array.prototype.splice.call(arguments, 0);

        this.emit(_eventDefinitions.REQUEST, frames[0], frames);

        var workerAddress = this._availableWorkers.shift();
        if (workerAddress) {
            this._sendToWorker(workerAddress, frames);
        } else {
            this._pendingRequests.push(frames);
        }
    }.bind(this);
};

/**
 * Disposes any resources that the queue may be holding on to.
 *
 * @method _dispose
 * @protected
 * @return {Function} A promise that will be resolved/rejected based on the
 *                    result of the dispose operation.
 */
SimpleQueue.prototype._dispose = function() {
    var def = _q.defer();
    def.resolve();
    return def.promise;
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
        this._backEnd.closeSocket(),
        this._dispose()
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
