/* jshint node:true, expr:true */
'use strict';

var _events = require('events');
var _zmq = require('zmq');
var _q = require('q');

/**
 * Class that encapsulates socket information, and provides methods to destroy
 * and reconnect the socket as necessary.
 *
 * @class SocketManager
 * @constructor
 * @param {String} type The socket type, used when creating the socket. Must
 *                 be a valid ZMQ socket type.
 * @param {String} endpoint The endpoint that the socket will connect/bind to.
 * @param {Object} [options] Optional socket options.
 */
function SocketManager(type, endpoint, options) {
    if (typeof type !== 'string' || type.length <= 0) {
        throw new Error('Invalid socket type specified (arg #1)');
    }

    if (typeof endpoint !== 'string' || endpoint.length <= 0) {
        throw new Error('Invalid endpoint specified (arg #2)');
    }
    this._type = type;
    this._endpoint = endpoint;
    this._options = options;
    this._bindPromise = null;
    this.socket = null;
}

/**
 * A reference to the socket created by the builder. Read only.
 *
 * @property
 * @readonly
 * @type Object
 * @default null
 */
SocketManager.prototype.socket = null;

/**
 * Creates a new socket object and binds it to its endpoint. Cannot be called
 * multiple times unless a `dispose()` is invoked first.
 *
 * @method bindSocket
 * @return {Function} A CommonJS promise that will be resolved/rejected
 *                    based on whether the bind succeeds or fails.
 */
SocketManager.prototype.bindSocket = function() {
    if (this.socket !== null) {
        throw new Error('Cannot bind/connect. Socket has already been initialized.');
    }

    var deferred = _q.defer();
    this.socket = _zmq.createSocket(this._type, this._options);
    this.socket.bind(this._endpoint, function(err) {
        if (err) {
            deferred.reject(err);
        } else {
            deferred.resolve(this.socket);
        }
    }.bind(this));

    this._bindPromise = deferred.promise;
    return deferred.promise;
};

/**
 * Creates a new socket object and connects it to its endpoint. Cannot be called
 * multiple times unless a `dispose()` is invoked first.
 *
 * @method connectSocket
 */
SocketManager.prototype.connectSocket = function() {
    if (this.socket !== null) {
        throw new Error('Cannot bind/connect. Socket has already been initialized.');
    }
    this.socket = _zmq.createSocket(this._type, this._options);
    this.socket.connect(this._endpoint);
};

/**
 * Closes the underlying socket and sets the `socket` property to null. Must be
 * invoked before `bindSocket()` or `connectSocket()` is called for a second
 * time.
 *
 * @method closeSocket
 * @return {Function} A CommonJS promise that will be resolved/rejected
 *                    based on whether the close operation succeeds or fails.
 */
SocketManager.prototype.closeSocket = function() {
    var deferred = _q.defer();

    var close = function() {
        try {
            if (this.socket !== null) {
                this.socket.close();
            }
            this.socket = null;
            this._bindPromise = null;
            deferred.resolve();
        } catch (err) {
            deferred.reject(err);
        }
    }.bind(this);

    if (this._bindPromise !== null) {
        this._bindPromise.fin(close);
    } else {
        close();
    }

    return deferred.promise;
};

module.exports = SocketManager;
