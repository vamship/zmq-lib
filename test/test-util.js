/* jshint node:true, expr:true */
'use strict';

module.exports = {
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
        } catch (ex) {
            done(ex);
        }
    }
}
