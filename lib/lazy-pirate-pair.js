/* jshint node:true, expr:true */
'use strict';

var _events = require('events');
var _util = require('util');
var _zmq = require('zmq');
var _q = require('q');
var _uuid = require('uuid');

var _messageDefinitions = require('./message-definitions');
var _eventDefinitions = require('./event-definitions');
var Monitor = require('./monitor');
var SocketManager = require('./socket-manager')

function _createMessageHandler(instance) {
    return function() {
        var frames = Array.prototype.splice.call(arguments, 0);
        var action = frames[0].toString();
        if(action === _messageDefinitions.ACKNOWLEDGE) {
            var messageId = frames[1].toString();
            delete this._messageMap[messageId];
        } else {
            this.emit(_eventDefinitions.DATA, frames);
        }
    }.bind(instance);
}

function _createRetryChecker(instance) {
    return function(maxRetriesExceeded) {
        var currentTime = Date.now();
        var messageId = null;

        var abandonedMessages = [];
        for(messageId in this._messageMap) {
            var messageInfo = this._messageMap[messageId];
            if(currentTime - messageInfo.timestamp > this._requestTimeout) {
                messageInfo.timestamp = currentTime;
                messageInfo.retries++;
                if(messageInfo.retries < this._retryCount) {
                    this._send(messageInfo.payload);
                } else {
                    abandonedMessages.push(messageId);
                }
            }
        }

        for(var index=0; index<abandonedMessages.length; index++) {
            messageId = abandonedMessages[index];
            delete this._messageMap[messageId];
            this.emit(_eventDefinitions.ABANDONED, messageId);
        }

        this._monitor.start(this._retryChecker);
    }.bind(instance);
}

/**
 * Lazy Pirate Client implementation for peer sockets, allowing for full duplex
 * communication between two peers with retry mechansims built in. Given that
 * TCP is not really conducive to true peer-peer connections, one peer must
 * function like a server, first binding to an endpoint, while the other must
 * connect to the "server" after it is listening on the endpoint.
 *
 * `ready`: Raised when the socket is ready to send data to the client. If a
 *          request was previously sent to the peer, the event handler will
 *          receive the response as the first argument.
 * `abandoned`: Raised when the socket is abandoning retry attempts to the peer.
 *
 * @class LazyPiratePeer
 * @constructor
 * @param {String} endpoint The endpoint that the client will connect to.
 * @param {Object} monitor A reference to a preinitialized monitoring object
 *                 that will be used to monitor responses from the peer that
 *                 this socket connects to.
 * @param {Object} options An options object that specifies parameters including
 *                 request timeout and retry count.
 * @param {Object} [socketOptions] Optional ZMQ options object, used to initialize the
 *                 socket.
 */
function LazyPiratePeer(endpoint, monitor, options, socketOptions) {
    if (typeof endpoint !== 'string' || endpoint.length <= 0) {
        throw new Error('Invalid endpoint specified (arg #1)');
    }
    if (!(monitor instanceof Monitor)) {
        throw new Error('Invalid retry monitor specified (arg #2)');
    }
    if (!options || typeof options !== 'object') {
        throw new Error('Invalid options specified (arg #3)');
    }
    if(typeof options.requestTimeout !== 'number' || options.requestTimeout <= 0) {
        throw new Error('Options does not define a valid request timeout value (options.requestTimeout)');
    }
    if(typeof options.retryCount !== 'number' || options.retryCount < 0) {
        throw new Error('Options does not define a valid retry count value (options.retryCount)');
    }
    _events.EventEmitter.call(this);

    this._socketManager = new SocketManager('pair', endpoint, socketOptions);
    this._messageHandler = _createMessageHandler(this);
    this._monitor = monitor;
    this._isServer = !!options.isServer;
    this._requestTimeout = options.requestTimeout;
    this._retryCount = options.retryCount;
    this._retryChecker = _createRetryChecker(this);

    this._monitor.start(this._retryChecker);
    this._messageMap = {};
}

_util.inherits(LazyPiratePeer, _events.EventEmitter);

/**
 * @method _initialize
 * @private
 */
LazyPiratePeer.prototype._initialize = function(sendEvent) {
    var promise = null;
    if(this._isServer) {
        promise = this._socketManager.bindSocket();
    } else {
        var def = _q.defer();
        try {
            this._socketManager.connectSocket();
            def.resolve(this);
        } catch (ex) {
            def.reject(ex);
        }
        promise = def.promise;
    }

    return promise.then(function() {
        this._socketManager.socket.on('message', this._messageHandler);
        if (sendEvent) {
            this.emit(_eventDefinitions.READY, this);
        }
    }.bind(this));
};

/**
 * @method _dispose
 * @private
 */
LazyPiratePeer.prototype._dispose = function() {
    this._socketManager.closeSocket();
    this._monitor.clear();
};
//
/**
 * @method _send
 * @private
 */
LazyPiratePeer.prototype._send = function(message) {
    this._socketManager.socket.send(message);
};

/**
 * Initializes the socket, and prepares it to send data to the client. This
 * method must only be invoked for the first time when setting up the initial
 * connection to a peer using a new object.
 *
 * @method initialize
 */
LazyPiratePeer.prototype.initialize = function() {
    return this._initialize(true);
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
LazyPiratePeer.prototype.send = function(message) {
    if (this._socketManager.socket === null) {
        throw new Error('Socket not initialized. Cannot send message');
    }
    var messageId = _uuid.v4();
    var messageInfo = {
        payload: [_messageDefinitions.REQUEST, messageId, message],
        timestamp: Date.now(),
        retries: 0
    };
    this._messageMap[messageId] = messageInfo;
    this._send(messageInfo.payload);
    return messageId;
};

/**
 * Acknowledges a message received from a peer.
 *
 * @method send
 * @param {String} messageId The id of the message to acknowledge
 */
LazyPiratePeer.prototype.acknowledge = function(messageId) {
    if (this._socketManager.socket === null) {
        throw new Error('Socket not initialized. Cannot send message');
    }
    if (typeof messageId !== 'string' || messageId.length <= 0) {
        throw new Error('Invalid message id specified (arg #1)');
    }
    this._send([ _messageDefinitions.ACKNOWLEDGE, messageId ]);
};

/**
 * Disposes the client, and closes any open sockets.
 *
 * @method dispose
 */
LazyPiratePeer.prototype.dispose = function() {
    this._dispose();
};

module.exports = LazyPiratePeer;
