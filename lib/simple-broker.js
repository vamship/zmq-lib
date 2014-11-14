/* jshint node:true, expr:true */
'use strict';

var _zmq = require('zmq');
var _q = require('q');
var SocketManager = require('./socket-manager');

/**
 * Simple broker class that accepts requests from the front end and routes them
 * to the first available worker on the back end.
 *
 * @class SimpleBroker
 * @constructor
 * @param {String} feEndpoint The endpoint for the front end of the broker.
 * @param {String} beEndpoint The endpoint for the back end of the broker.
 * @param {Object} [feOptions] Optional ZMQ options object, used to initialize
 *                 the front end socket of the broker.
 * @param {Object} [beOptions] Optional ZMQ options object, used to initialize
 *                 the back end socket of the broker.
 */
function SimpleBroker(feEndpoint, beEndpoint, feOptions, beOptions) {
    if(typeof feEndpoint !== 'string' || feEndpoint.length <= 0) {
        throw new Error('Invalid front end endpoint specified (arg #1)');
    }
    if(typeof beEndpoint !== 'string' || beEndpoint.length <= 0) {
        throw new Error('Invalid back end endpoint specified (arg #2)');
    }
    this._isReady = false;
    this._pendingRequests = [];
    this._availableWorkers = [];

    // Initialize front end.
    this._frontEnd = new SocketManager('router', feEndpoint, feOptions);
    this._feMessageHandler = function() {
        var frames = Array.prototype.splice.call(arguments, 0);
        var worker = this._availableWorkers.shift();
        if(worker) {
           this._backEnd.socket.send(worker.concat(frames));
        } else {
           this._pendingRequests.push(frames);
        }
    }.bind(this);

    // Initialize back end.
    this._backEnd = new SocketManager('router', beEndpoint, beOptions);
    this._beMessageHandler = function() {
        var frames = Array.prototype.splice.call(arguments, 0);
        var worker = [ frames[0], frames[1] ];
        var request = this._pendingRequests.shift();

        if(frames[2].toString() !== 'READY') {
            var response = [ ];
            for(var index=2; index<frames.length; index++){
                response.push(frames[index]);
            }
            this._frontEnd.socket.send(response);
        }
        if(request) {
            this._backEnd.socket.send(worker.concat(request));
        } else {
            this._availableWorkers.push(worker);
        }
    }.bind(this);
}

/**
 * Initializes the broker, binding the front and back end sockets to their
 * respective endpoints.
 *
 * @method initialize
 */ 
SimpleBroker.prototype.initialize = function() {
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
 * Disposes the broker, closing both front end and back end sockets.
 *
 * @method dispose
 */
SimpleBroker.prototype.dispose = function() {
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
 * Retrieves the number of un assigned requests matintained by the broker.
 *
 * @return {Number} The current count of unaasigned requests.
 */
SimpleBroker.prototype.getPendingRequestCount = function() {
    return this._pendingRequests.length;
};

/**
 * Retrieves the number of free workers available to the broker.
 *
 * @return {Number} The current count of available workers.
 */
SimpleBroker.prototype.getAvailableWorkerCount = function() {
    return this._availableWorkers.length;
};

/**
 * Returns a boolean value that determines whether or not the broker is ready.
 *
 * @return {Boolean} True if the broker is initialized and ready, false 
 *                   otherwise.
 */
SimpleBroker.prototype.isReady = function() {
    return this._isReady;
};

module.exports = SimpleBroker;
