/* jshint node:true, expr:true */
'use strict';

/**
 * General purpose timer that invokes callbacks when specific timeout periods
 * expire. This can be used to monitor responses from sockets, or check for
 * heartbeat expiries, etc.
 *
 * @class Monitor
 * @constructor
 * @param {Number} frequency The duration in milliseconds after which the 
 *                 monitor will invoke the callback.
 * @param {Number} maxExpiryCount The maximum number of times that the 
 *                 timer can expire.
 */
function Monitor(frequency, maxExpiryCount) {
    if(typeof(frequency) !== 'number' || frequency <= 0) {
        throw new Error('Invalid frequency specified (arg #1)');
    }
    if(typeof(maxExpiryCount) !== 'number' && maxExpiryCount !== null &&
                                  typeof(maxExpiryCount) !== 'undefined') {
        throw new Error('Invalid max hit count specified (arg #2)');
    }

    this._maxExpiryCount = (!maxExpiryCount || maxExpiryCount <= 0) ?
                                                        -1 : maxExpiryCount;
    this._frequency = frequency;

    this._timerRef = null;
    this._expiryCount = 0;
}

/**
 * @method _clearTimer
 * @private
 */
Monitor.prototype._clearTimer = function() {
    if(this._timerRef !== null) {
        clearTimeout(this._timerRef);
        this._timerRef = null;
    }
}

/**
 * Triggers the start of a monitoring cycle, resulting in the callback method
 * being invoked once the timer expires.
 *
 * @method start
 * @param {Function} callback The callback method that will be invoked after the
 *                   timer expires. The first parameter of this callback will be
 *                   a boolean that will be true if the maximum number of
 *                   expiries has been exceeded.
 */
Monitor.prototype.start = function(callback) {
    if(typeof callback !== 'function') {
        throw new Error('Invalid callback specified (arg #1)');
    }

    this._timerRef = setTimeout(function() {
        this._expiryCount++;
        callback(this._expiryCount > this._maxExpiryCount);
    }.bind(this), this._frequency);

};

/**
 * Clears an existing timer. Can be invoked to cancel an existing monitoring
 * cycle.
 *
 * @method clear
 */
Monitor.prototype.clear = function() {
    this._clearTimer();
    this._expiryCount = 0;
};

/**
 * Gets the number of times that the monitoring cycle has expired since the
 * last reset.
 *
 * @method getExpiryCount
 * @return {Number} The number of times that the monitoring cycle has expired.
 */
Monitor.prototype.getExpiryCount = function() {
    return this._expiryCount;
};

/**
 * Returns a value that determines whether or not a monitoring cycle is in
 * progress.
 *
 * @method isInProgress
 * @return {Boolean} True if a monitoring cycle is in progress, false
 *                   otherwise.
 */
Monitor.prototype.isInProgress = function() {
    return this._timerRef !== null;
};

/**
 * Gets the monitoring frequency.
 *
 * @method getFrequency
 * @return {Number} The number of milliseconds after which the monitoring timer
 *                  expires.
 */
Monitor.prototype.getFrequency = function() {
    return this._frequency;
};

/**
 * Gets the maximum number of expiries allowed
 *
 * @method getMaxExpiryCount
 * @return {Number} The number of times that the monitoring timer can expire
 *                  before an error is reported.
 */
Monitor.prototype.getMaxExpiryCount = function() {
    return this._maxExpiryCount;
};

module.exports = Monitor;
