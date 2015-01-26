/* jshint node:true, expr:true */
'use strict';

module.exports = {
    /**
     * Defines the message used by a socket to signal to a peer that it is
     * ready.
     *
     * @property READY
     * @type {String}
     * @readonly
     */
    READY: 'READY',

    /**
     * Defines the message used by a socket to signal to a peer that the
     * remainder of the message constitutes a request.
     *
     * @property REQUEST
     * @type {String}
     * @readonly
     */
    REQUEST: 'REQUEST',

    /**
     * Defines the message used by a socket to signal to a peer that the
     * remainder of the message constitutes a response to a request.
     *
     * @property RESPONSE
     * @type {String}
     * @readonly
     */
    RESPONSE: 'RESPONSE',

    /**
     * Defines the message used by a socket to signal to a peer that it is
     * alive and accessible.
     *
     * @property READY
     * @type {String}
     * @readonly
     */
    HEARTBEAT: 'HEARTBEAT'
}
