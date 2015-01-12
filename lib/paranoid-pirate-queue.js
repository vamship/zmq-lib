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
        var session = null;
        var index = 0;

        var expiredWorkers = [];
        for (var workerId in this._workerMap) {
            worker = this._workerMap[workerId];
            if (now - worker.lastAccess > this._workerTimeout) {
                expiredWorkers.push(worker);
            }
        }

        for (index = 0; index < expiredWorkers.length; index++) {
            worker = expiredWorkers[index];
            delete this._workerMap[worker.id];
        }

        if(this._session.isEnabled) {
            var expiredSessions = [];
            for(var sessionId in this._sessionMap) {
                session = this._sessionMap[sessionId];
                if (now - session.lastAccess > this._session.timeout) {
                    expiredSessions.push(session);
                }
            }

            for (index = 0; index < expiredSessions.length; index++) {
                session = expiredSessions[index];
                delete this._sessionMap[session.id];
            }
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
 * @param {Object} queueOptions Options object that determines how the queue
 *                 treats workers, requests and client sessions
 * @param {Object} [feOptions] Optional ZMQ options object, used to initialize
 *                 the front end socket of the queue.
 * @param {Object} [beOptions] Optional ZMQ options object, used to initialize
 *                 the back end socket of the queue.
 */
function ParanoidPirateQueue(feEndpoint, beEndpoint, queueOptions, feOptions, beOptions) {
    if (typeof feEndpoint !== 'string' || feEndpoint.length <= 0) {
        throw new Error('invalid front end endpoint specified (arg #1)');
    }
    if (typeof beEndpoint !== 'string' || beEndpoint.length <= 0) {
        throw new Error('invalid back end endpoint specified (arg #2)');
    }
    if (!queueOptions || typeof(queueOptions) !== 'object') {
        throw new Error('invalid queue options specified (arg #3)');
    }
    if (typeof queueOptions.pollFrequency !== 'number' || queueOptions.pollFrequency <= 0) {
        throw new Error('queue options does not define a poll frequency property (queueOptions.pollFrequency)');
    }
    if (typeof queueOptions.workerTimeout !== 'number' || queueOptions.workerTimeout <= 0) {
        throw new Error('queue options does not define a worker timeout property (queueOptions.workerTimeout)');
    }
    if (typeof queueOptions.requestTimeout !== 'number' || queueOptions.requestTimeout <= 0) {
        throw new Error('queue options does not define a request timeout property (queueOptions.requestTimeout)');
    }

    ParanoidPirateQueue.super_.call(this, feEndpoint, beEndpoint, feOptions, beOptions);

    var session = queueOptions.session || {};
    this._session = {
        isEnabled: (typeof session.timeout === 'number'),
        timeout: session.timeout
    };

    this._monitor = new Monitor(queueOptions.pollFrequency, 3);
    this._workerTimeout = queueOptions.workerTimeout;
    this._workerMap = {};
    this._sessionMap = {};
    this._availableWorkerCount = 0;
    this._pendingRequestCount = 0;

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
 * @param {Object} worker An object that represents the worker to whom the
 *                        data will be sent.
 * @param {Object} clientSession An object that represents the client session
 *                               associated with the request.
 * @param {Array} request An array of buffers that represents the data payload to be
 *                        sent to the worker.
 */
ParanoidPirateQueue.prototype._sendToWorker = function(worker, clientSession, request) {
    if(clientSession && clientSession.workerId.length <= 0) {
        clientSession.workerId = worker.id;
    }
    worker.isAvailable = false;

    request.unshift(_messageDefinitions.REQUEST);
    request.unshift(worker.address);
    this._backEnd.socket.send(request);
    this.emit(_eventDefinitions.ASSIGNED_REQUEST, worker.address, request);
};

/**
 * Returns a client session for the given request, provided sessions are
 * enabled.
 *
 * @method _getClientSession
 * @protected
 * @param {Array} frames An array of frames that represents the client request.
 * @return {Object} An object that represents the client session. If session
 *                  is not enabled, a null will be returned.
 */
ParanoidPirateQueue.prototype._getClientSession = function(frames) {
    var clientSession = null;
    if(this._session.isEnabled) {
        var clientAddress = frames[0];
        var clientId = clientAddress.toString('base64');
        clientSession = this._sessionMap[clientId];
        if(!clientSession) {
            clientSession = {
                id: clientId,
                lastAccess: Date.now(),
                workerId: ''
            };
        }
        this._sessionMap[clientId] = clientSession;
    }
    return clientSession;
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

        var clientSession = this._getClientSession(frames);
        var worker = null;
        if(clientSession) {
            clientSession.lastAccess = Date.now();
            worker = this._workerMap[clientSession.workerId];
        }

        if(!worker) {
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
        }

        var request = {
            requestTime: Date.now(),
            data: frames
        };

        if (worker) {
            if(worker.isAvailable) {
                this._availableWorkerCount--;
                this._sendToWorker(worker, clientSession, request.data);
            } else {
                this._pendingRequestCount++;
                worker.pendingRequests.push(request);
            }
        } else {
            this._pendingRequestCount++;
            this._pendingRequests.push(request);
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
                worker.lastAccess = Date.now();
                this._backEnd.socket.send([workerAddress, _messageDefinitions.HEARTBEAT]);
                // Return from the function and stop processing anything further.
                return;
            case _messageDefinitions.READY:
                worker = {
                    id: workerId,
                    address: workerAddress,
                    isAvailable: true,
                    pendingRequests: [],
                    lastAccess: Date.now()
                };
                this._workerMap[workerId] = worker;
                break;
            default:
                worker.lastAccess = Date.now();
                worker.isAvailable = true;
                this._frontEnd.socket.send(frames);
                this.emit(_eventDefinitions.ASSIGNED_RESPONSE, frames[0], frames);
                break;
        }

        var request = worker.pendingRequests.shift();
        if(!request) {
            request = this._pendingRequests.shift();
        }
        if (request) {
            var clientSession = this._getClientSession(request.data);
            this._pendingRequestCount--;

            this._sendToWorker(worker, clientSession, request.data);
        } else {
            this._availableWorkerCount++;
            this._availableWorkers.push(worker);
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
ParanoidPirateQueue.prototype._dispose = function() {
    var def = _q.defer();
    this._monitor.clear();
    def.resolve();
    return def.promise;
}

/**
 * Returns the map of all active sessions that the queue is aware of. Note that
 * the returned object is a clone of the original, and will not be updated over
 * time.
 *
 * @method getSessionMap
 * @return {Object} A hash containing all known active sessions, along with some
 *                  metadata (client id, last used, etc.).
 */
ParanoidPirateQueue.prototype.getSessionMap = function() {
    return _clone(this._sessionMap);
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
    var map = {};
    for(var workerId in this._workerMap) {
        var worker = this._workerMap[workerId];
        map[workerId] = {
            id: worker.id,
            address: worker.address,
            lastAccess: worker.lastAccess,
            isAvailable: worker.isAvailable,
            pendingRequestCount: worker.pendingRequests.length
        };
    }
    return map;
};

/**
 * Retrieves the number of pending requests on the queue.
 *
 * @protected
 * @return {Number} The current count of available workers.
 */
ParanoidPirateQueue.prototype.getPendingRequestCount = function() {
    return this._pendingRequestCount;
};

/**
 * Retrieves the number of free workers available to the queue.
 *
 * @protected
 * @return {Number} The current count of available workers.
 */
ParanoidPirateQueue.prototype.getAvailableWorkerCount = function() {
    return this._availableWorkerCount;
};

module.exports = ParanoidPirateQueue;
