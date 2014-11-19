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
     * An event that indicates that a socket has received a request from a
     * peer.
     *
     * @property REQUEST
     * @type {String}
     * @readonly
     */
    REQUEST: 'request'
}
