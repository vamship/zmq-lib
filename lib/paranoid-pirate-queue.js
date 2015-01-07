/* jshint node:true, expr:true */
'use strict';

var _events = require('events');
var _zmq = require('zmq');
var _q = require('q');
var _util = require('util');
var _clone = require('clone');

var SimpleQueue = require('./simple-queue');
var Monitor = require('./monitor');
var SocketManager = require('./socket-manager');
var _messageDefinitions = require('./message-definitions');
var _eventDefinitions = require('./event-definitions');

var EMPTY_BUFFER = new Buffer(0);

function _createWorkerMonitor(instance) {
    return function(maxRetriesExceeded) {
        var now = Date.now();
        var worker = null;

        var expiredWorkers = [];
        for (var workerId in this._workerMap) {
            worker = this._workerMap[workerId];
            if (now - worker.lastContact > this._workerTimeout) {
                expiredWorkers.push(worker);
            }
        }

        for (var index = 0; index < expiredWorkers.length; index++) {
            worker = expiredWorkers[index];
            delete this._workerMap[worker.id];
        }

        this._availableWorkerCount -= expiredWorkers.length;

        this._monitor.resetExpiryCount();
        this._monitor.start(this._workerMonitor);
    }.bind(instance);
}

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
 * @param {Object} workerOptions Options object that determines how the queue
 *                 treats workers that miss heartbeats
 * @param {Object} [feOptions] Optional ZMQ options object, used to initialize
 *                 the front end socket of the queue.
 * @param {Object} [beOptions] Optional ZMQ options object, used to initialize
 *                 the back end socket of the queue.
 */
function ParanoidPirateQueue(feEndpoint, beEndpoint, workerOptions, feOptions, beOptions) {
    if (typeof feEndpoint !== 'string' || feEndpoint.length <= 0) {
        throw new Error('invalid front end endpoint specified (arg #1)');
    }
    if (typeof beEndpoint !== 'string' || beEndpoint.length <= 0) {
        throw new Error('invalid back end endpoint specified (arg #2)');
    }
    if (!workerOptions || typeof(workerOptions) !== 'object') {
        throw new Error('invalid worker options specified (arg #3)');
    }
    if (typeof workerOptions.pollFrequency !== 'number' || workerOptions.pollFrequency <= 0) {
        throw new Error('worker options does not define a poll frequency property (workerOptions.pollFrequency)');
    }
    if (typeof workerOptions.workerTimeout !== 'number' || workerOptions.workerTimeout <= 0) {
        throw new Error('worker options does not define a worker timeout property (workerOptions.workerTimeout)');
    }

    ParanoidPirateQueue.super_.call(this, feEndpoint, beEndpoint, feOptions, beOptions);

    this._monitor = new Monitor(workerOptions.pollFrequency, 3);
    this._workerTimeout = workerOptions.workerTimeout;
    this._workerMap = {};
    this._availableWorkerCount = 0;

    this._workerMonitor = _createWorkerMonitor(this);
    this._monitor.start(this._workerMonitor);
}

_util.inherits(ParanoidPirateQueue, SimpleQueue);

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
ParanoidPirateQueue.prototype._sendToWorker = function(workerAddress, request) {
    request.unshift(_messageDefinitions.REQUEST);
    request.unshift(workerAddress);
    this._backEnd.socket.send(request);
    this.emit(_eventDefinitions.ASSIGNED_REQUEST, workerAddress, request);
};

/**
 * Returns a handler for messages from clients.
 *
 * @method _getFrontEndMessageHandler
 * @protected
 * @return {Function} A handler that will receive and process messages from the
 *                    frontend (i.e., clients).
 */
ParanoidPirateQueue.prototype._getFrontEndMessageHandler = function() {
    return function() {
        var frames = Array.prototype.splice.call(arguments, 0);

        this.emit(_eventDefinitions.REQUEST, frames[0], frames);
        var worker = null;

        //Loop until the next available worker is found.
        while (this._availableWorkers.length > 0) {
            worker = this._availableWorkers.shift();
            if (worker) {
                //If we can't find an entry in the worker map, it
                //means that the worker has expired and has been 
                //removed.
                worker = this._workerMap[worker.id];
                if (worker) {
                    break;
                }
            }
        }

        if (worker) {
            this._availableWorkerCount--;
            this._sendToWorker(worker.address, frames);
        } else {
            this._pendingRequests.push(frames);
        }
    }.bind(this);
};

/**
 * Returns a handler for messages from workers.
 *
 * @method _getBackEndMessageHandler
 * @protected
 * @return {Function} A handler that will receive and process messages from the
 *                    backend (i.e., workers).
 */
ParanoidPirateQueue.prototype._getBackEndMessageHandler = function() {
    return function() {
        var frames = Array.prototype.splice.call(arguments, 0);
        // Pop out the topmost frame (worker address)
        var workerAddress = frames.shift();

        if (frames.length <= 0) {
            // Bad message from worker. Discard it.
            return;
        }

        var workerId = workerAddress.toString('base64');
        var action = frames[0].toString();
        var worker = this._workerMap[workerId];

        switch (action) {
            case _messageDefinitions.HEARTBEAT:
                worker.lastContact = Date.now();
                this._backEnd.socket.send([workerAddress, _messageDefinitions.HEARTBEAT]);
                // Return from the function and stop processing anything further.
                return;
            case _messageDefinitions.READY:
                worker = {
                    id: workerId,
                    address: workerAddress,
                    lastContact: Date.now()
                };
                this._workerMap[workerId] = worker;
                break;
            default:
                worker.lastContact = Date.now();
                this._frontEnd.socket.send(frames);
                this.emit(_eventDefinitions.ASSIGNED_RESPONSE, frames[0], frames);
                break;
        }

        var request = this._pendingRequests.shift();
        if (request) {
            this._sendToWorker(workerAddress, request);
        } else {
            this._availableWorkerCount++;
            this._availableWorkers.push(worker);
        }
    }.bind(this);
};

/**
 * Returns the map of all workers that the queue is aware of. Note that the
 * returned object is a clone of the original, and will not be updated
 * over time.
 *
 * @method getWorkerMap
 * @return {Object} A hash containing all known workers, along with some
 *                  metadata (last heartbeat, etc.).
 */
ParanoidPirateQueue.prototype.getWorkerMap = function() {
    return _clone(this._workerMap);
};

/**
 * Retrieves the number of free workers available to the queue.
 *
 * @return {Number} The current count of available workers.
 */
ParanoidPirateQueue.prototype.getAvailableWorkerCount = function() {
    return this._availableWorkerCount;
};

module.exports = ParanoidPirateQueue;
