/* jshint node:true, expr:true */
'use strict';

var _chai = require('chai');
var expect = _chai.expect;

var _testUtils = require('../test-util');
var Monitor = require('../../lib/monitor');

describe('Monitor', function() {
    var DEFAULT_FREQUENCY = 10;
    var DEFAULT_MAX_HIT_COUNT = 3;

    describe('ctor()', function() {
        it('should throw an error if a valid frequency is not specified', function() {
            var error = 'Invalid frequency specified (arg #1)';

            function createMonitor(frequency) {
                return new Monitor(frequency);
            }

            expect(function() { createMonitor() }).to.throw(error);
            expect(function() { createMonitor(null) }).to.throw(error);
            expect(function() { createMonitor('') }).to.throw(error);
            expect(function() { createMonitor(0) }).to.throw(error);
            expect(function() { createMonitor(true) }).to.throw(error);
            expect(function() { createMonitor([]) }).to.throw(error);
            expect(function() { createMonitor({}) }).to.throw(error);
        });

        it('should throw an error if a valid max hit count is not specified', function() {
            var error = 'Invalid max hit count specified (arg #2)';

            function createMonitor(maxRetries) {
                return new Monitor(DEFAULT_FREQUENCY, maxRetries);
            }

            expect(function() { createMonitor('') }).to.throw(error);
            expect(function() { createMonitor(true) }).to.throw(error);
            expect(function() { createMonitor([]) }).to.throw(error);
            expect(function() { createMonitor({}) }).to.throw(error);
        });

        it('should create an object that exposes members required by the interface', function() {
            var monitor = new Monitor(DEFAULT_FREQUENCY, DEFAULT_MAX_HIT_COUNT);

            expect(monitor).to.be.an('object');
            expect(monitor).to.have.property('start').and.to.be.a('function');
            expect(monitor).to.have.property('clear').and.to.be.a('function');
            expect(monitor).to.have.property('getExpiryCount').and.to.be.a('function');
            expect(monitor).to.have.property('getFrequency').and.to.be.a('function');
            expect(monitor).to.have.property('getMaxExpiryCount').and.to.be.a('function');
            expect(monitor).to.have.property('isInProgress').and.to.be.a('function');
        });

        it('should initialize property values correctly values when only the frequency is specified', function() {
            var monitor = new Monitor(DEFAULT_FREQUENCY);
            
            expect(monitor.getExpiryCount()).to.equal(0);
            expect(monitor.isInProgress()).to.be.false;
            expect(monitor.getFrequency()).to.equal(DEFAULT_FREQUENCY);
            expect(monitor.getMaxExpiryCount()).to.equal(-1);
        });

        it('should initialize property values correctly values when both frequency and max retries are specified', function() {
            function evaluateCtor(freq, maxCount) {
                var monitor = new Monitor(freq, maxCount);
                
                expect(monitor.getExpiryCount()).to.equal(0);
                expect(monitor.isInProgress()).to.be.false;
                expect(monitor.getFrequency()).to.equal(freq);
                expect(monitor.getMaxExpiryCount()).to.equal(maxCount);
            }

            evaluateCtor(DEFAULT_FREQUENCY, DEFAULT_MAX_HIT_COUNT);
            evaluateCtor(100, 3);
            evaluateCtor(200, 5);

        });
    });

    describe('start()', function() {
        it('should throw an error if a valid callback is not specified', function() {
            var error = 'Invalid callback specified (arg #1)';
            var monitor = new Monitor(DEFAULT_FREQUENCY, DEFAULT_MAX_HIT_COUNT);

            function invokeStart(callback) {
                monitor.start(callback);
            }

            expect(function() { invokeStart() }).to.throw(error);
            expect(function() { invokeStart(null) }).to.throw(error);
            expect(function() { invokeStart('foo') }).to.throw(error);
            expect(function() { invokeStart(0) }).to.throw(error);
            expect(function() { invokeStart(true) }).to.throw(error);
            expect(function() { invokeStart([]) }).to.throw(error);
            expect(function() { invokeStart({}) }).to.throw(error);
        });

        it('should invoke the callback parameter once the frequency duration expires', function(done) {
            var monitor = new Monitor(DEFAULT_FREQUENCY, DEFAULT_MAX_HIT_COUNT);

            var startTime = Date.now();
            monitor.start(function() {
                var endTime = Date.now();
                var min = DEFAULT_FREQUENCY;
                var max = DEFAULT_FREQUENCY + 3;
                _testUtils.evaluateExpectations(function(){
                    expect(endTime - startTime).to.be.within(min, max);
                }, done);
            });
        });

        it('should increment the expiry count once the frequency duration expires', function(done) {
            var monitor = new Monitor(DEFAULT_FREQUENCY, DEFAULT_MAX_HIT_COUNT);

            expect(monitor.getExpiryCount()).to.equal(0);

            var count = 0;
            function evaluateExpiryCount() {
                count++;
                _testUtils.evaluateExpectations(function(){
                    expect(monitor.getExpiryCount()).to.equal(count);
                }, done, true);

                if(count < DEFAULT_MAX_HIT_COUNT) {
                    monitor.start(evaluateExpiryCount);
                } else {
                    done();
                }
            }
            monitor.start(evaluateExpiryCount);
        });

        it('should invoke the callback with the expiryLimitExceeded=false if the expiryCount <= maxExpiryCount', function(done) {
            var monitor = new Monitor(DEFAULT_FREQUENCY, DEFAULT_MAX_HIT_COUNT);

            var count = 0;
            function evaluateLimitExceeded(limitExceeded) {
                count++;
                _testUtils.evaluateExpectations(function(){
                    expect(limitExceeded).to.be.false;
                }, done, true);

                if(count < DEFAULT_MAX_HIT_COUNT) {
                    monitor.start(evaluateLimitExceeded);
                } else {
                    done();
                }
            }
            monitor.start(evaluateLimitExceeded);
        });

        it('should invoke the callback with the expiryLimitExceeded=true if the expiryCount > maxExpiryCount', function(done) {
            var monitor = new Monitor(DEFAULT_FREQUENCY, DEFAULT_MAX_HIT_COUNT);

            var count = 0;
            function evaluateLimitExceeded(limitExceeded) {
                count++;
                if(count <= DEFAULT_MAX_HIT_COUNT) {
                    monitor.start(evaluateLimitExceeded);
                } else if(count > DEFAULT_MAX_HIT_COUNT) {
                    _testUtils.evaluateExpectations(function(){
                        expect(limitExceeded).to.be.true;
                    }, done, true);
                    done();
                }
            }
            monitor.start(evaluateLimitExceeded);
        });
    });

    describe('clear()', function(done) {
        it('should reset the expiry count to 0 when invoked', function(done) {
            var monitor = new Monitor(DEFAULT_FREQUENCY, DEFAULT_MAX_HIT_COUNT);

            monitor.start(function() {
                _testUtils.evaluateExpectations(function(){
                    expect(monitor.getExpiryCount()).to.equal(1);
                    monitor.clear();
                    expect(monitor.getExpiryCount()).to.equal(0);
                }, done);
            });
        });

        it('should set the in progress flag to false when invoked.', function() {
            var monitor = new Monitor(DEFAULT_FREQUENCY, DEFAULT_MAX_HIT_COUNT);

            monitor.start(function() {});
            monitor.clear();

            expect(monitor.isInProgress()).to.be.false;
        });
    });
});
