/* jshint node:true, expr:true */
'use strict';

module.exports = {
    /**
     * An event that indicates that a socket is ready to be used.
     *
     * @property READY
     * @type {String}
     * @readonly
     */
    READY: 'ready',

    /**
     * An event that indicates that a socket is abandoning retries. This
     * typically means that socket will no longer attempt any further
     * operations unless requested by some external code.
     *
     * @property ABANDONED
     * @type {String}
     * @readonly
     */
    ABANDONED: 'abandoned',

    /**
     * An event that indicates that a socket has been closed.
     *
     * @property CLOSED
     * @type {string}
     * @readonly
     */
    CLOSED: 'closed',

    /**
     * An event that indicates that a socket has received some data from a
     * peer. Typically used in peer to peer communication, where there is 
     * no established request-response pattern.
     *
     * @property DATA
     * @type {string}
     * @readonly
     */
    DATA: 'data',

    /**
     * An event that indicates that a socket has received a request from a
     * peer.
     *
     * @property REQUEST
     * @type {string}
     * @readonly
     */
    REQUEST: 'request',

    /**
     * An event that indicates that a socket has received a response to a request
     * from a peer.
     *
     * @property RESPONSE
     * @type {string}
     * @readonly
     */
    RESPONSE: 'response',

    /**
     * An event that indicates that a queue has assigned a request to a worker
     * socket.
     *
     * @property ASSIGNED_REQUEST
     * @type {string}
     * @readonly
     */
    ASSIGNED_REQUEST: 'assigned_request',

    /**
     * An event that indicates that a queue has assigned a response from a worker
     * back to the client.
     *
     * @property ASSIGNED_RESPONSE
     * @type {string}
     * @readonly
     */
    ASSIGNED_RESPONSE: 'assigned_response'
}
