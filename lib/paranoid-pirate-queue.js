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
 *                 dead (or non responsive) workers.
 * @param {Object} [workerOptions] Optional options object that determines how
 *                 the queue treats workers that miss heartbeats
 * @param {Object} [feOptions] Optional ZMQ options object, used to initialize
 *                 the front end socket of the queue.
 * @param {Object} [beOptions] Optional ZMQ options object, used to initialize
 *                 the back end socket of the queue.
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

    ParanoidPirateQueue.super_.call(this, feEndpoint, beEndpoint, feOptions, beOptions);
    this._knownWorkers = {};
}

_util.inherits(ParanoidPirateQueue, SimpleQueue);

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

module.exports = ParanoidPirateQueue;
