/* jshint node:true, expr:true */
'use strict';

var _util = require('util');
var _uuid = require('node-uuid');
var _q = require('q');

module.exports = {

    /**
     * Generates a callback that resolves done() without any errors.
     *
     * @param {Function} done Async test complete signaller
     * @return {Function} A callback that can be used to resolve done()
     */
    getSuccessCallback: function(done) {
        return function(){
            done();
        }
    },

    /**
     * Generates a callback that rejects done() without any errors.
     *
     * @param {Function} done Async test complete signaller
     * @return {Function} A callback that can be used to reject done()
     */
    getFailureCallback: function(done) {
        return function(err) {
            done(err);
        }
    },

    /**
     * Generates a random endpoint for sockets under test. The endpoints are
     * ipc endpoints (with prefix ipc://), with a random string for the
     * value.
     *
     * @return {String} An ipc endpoint string.
     */
    generateEndpoint: function() {
        return 'ipc://ep-' + _uuid.v4();
    },

    /**
     * Gets a method that executes the specified task after a delay. The method
     * also returns a promise that will be resolved or rejected based on the 
     * whether or not the task executed without errors.
     *
     * @param {Function} task The task to execute after delay, wrapped in a
     *                   parameterless function.
     * @param {Number} delay The delay (in milliseconds) after which the task
     *                 will be executed.
     *
     * @return {Function} A function that can be used to trigger the delayed 
     *                    execution of the task. This function will return 
     *                    a promise after execution.
     */
    getDelayedRunner: function(task, delay) {
        return function(data) {
            var expectations = function() {
                return task(data);
            };
            var deferred = _q.defer();
            setTimeout(function(){
                module.exports.runDeferred(expectations, deferred);
            }, delay);
            
            return deferred.promise;
        };
    },
    
    /**
     * Helper method that executes a function and rejects/resolves the passed
     * in deferred object based on whether or not the function executed without
     * errors. If the function returns a promise, eventual rejection/resolution
     * will be tied to the resolution/rejection of that promise (unless an
     * exception is thrown first).
     * 
     * @param {Function} tasks A parameterless function that encapsulates the
     *                   tasks to be executed.
     * @param {Object} [deferred] A deferred object that will be rejected or
     *                 resolved based on whether or not the tasks execute 
     *                 successfully. If omitted, a new deferred object will
     *                 be created and returned.
     * @return {Object} A promise that will be rejected or resolved based on
     *                  the status of execution of the tasks.
     */
    runDeferred: function(expectations, deferred) {
       deferred = deferred || _q.defer();
       try {
           var ret = expectations();
           // Poor man's check to see if the return value is a promise.
           if(ret && typeof ret.then === 'function') {
               ret.then(function(data) {
                   deferred.resolve(data);
               }, function(err) {
                   deferred.reject(err);
               });
           } else {
               deferred.resolve(ret);
           }
       } catch(ex) {
           if(ex.message) {
               deferred.reject(ex.message);
           } else {
               deferred.reject(_util.inspect(ex));
           }
       }
       return deferred;
    },

    /**
     * Helper method that evaluates expect statements within async callbacks.
     * This method does not execute asynchronously - it is merely a helper that
     * can be used within async methods in tests.
     *
     * @param {Function} expectations A callback method that contains the test
     *                   assertions/expectations.
     * @param {Function} done A callback method that can be used to signify the
     *                   the completion of the test case.
     * @param {Boolean} [continueExec=false] An optional parameter that tells
     *                  the helper to not invoke `done()` if the exepectations
     *                  execute without errors. Useful when further evaluations
     *                  need to be performed.
     */
    evaluateExpectations: function(expectations, done, continueExec) {
        try {
            expectations();
            if(!continueExec) {
                done();
            }
            return true;
        } catch (ex) {
            done(ex);
            return false;
        }
    },

    /**
     * Introduces a delay in exection
     *
     * @param {Number} delay The duration of the delay in milliseconds.
     * @returns {Object} A promise that will be resolved once the delay has
     *                   expired.
     */
    wait: function(delay) {
        var mod = module.exports;
        return mod.getDelayedRunner(function(data) {
            return data;
        }, delay);
    }
}
