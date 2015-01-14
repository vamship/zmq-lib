/* jshint node:true, expr:true */
'use strict';

var _events = require('events');
var _zmq = require('zmq');
var _q = require('q');
var _uuid = require('node-uuid');
var _sinon = require('sinon');
var _chai = require('chai');
_chai.use(require('sinon-chai'));
_chai.use(require('chai-as-promised'));

var expect = _chai.expect;
var _testUtils = require('../test-util');
var Monitor = require('../../lib/monitor');
var ParanoidPirateWorker = require('../../lib/paranoid-pirate-worker');
var _messageDefinitions = require('../../lib/message-definitions');
var _eventDefinitions = require('../../lib/event-definitions');

describe('ParanoidPirateWorker', function(){
    var DEFAULT_RETRY_FREQ = 2500;
    var DEFAULT_RETRY_COUNT = 3;
    var DEFAULT_BACKOFF_OPTIONS = { backoff: 10, retries: 3 };
    var _queue;
    var _worker;

    function _createPPWorker(endpoint, monitor, backoffOptions) {
        backoffOptions = backoffOptions || DEFAULT_BACKOFF_OPTIONS;
        monitor = monitor || new Monitor(DEFAULT_RETRY_FREQ, DEFAULT_RETRY_COUNT)
        var worker = new ParanoidPirateWorker(endpoint, monitor, backoffOptions);

        return worker;
    }

    function _createQueue(endpoint) {
        var socket = _zmq.createSocket('router');
        socket.monitor(10);
        socket.bind(endpoint);

        return socket;
    }

    function _getRange(value, delta) {
        return ([value - delta, value + delta]);
    }

    afterEach(function() {
        // Clean up resources. This will happen even if tests fail.
        if(_queue){
            _queue.close();
        }
        if(_worker){
            _worker.dispose();
        }
        _worker = null;
        _queue = null;
    });


    describe('ctor()', function() {
        it('should throw an error if a valid endpoint value is not specified', function() {
            var error = 'Invalid endpoint specified (arg #1)';

            function createWorker(endpoint) {
                return function() {
                    return new ParanoidPirateWorker(endpoint);
                };
            }

            expect(createWorker()).to.throw(error);
            expect(createWorker(null)).to.throw(error);
            expect(createWorker('')).to.throw(error);
            expect(createWorker(1)).to.throw(error);
            expect(createWorker(true)).to.throw(error);
            expect(createWorker([])).to.throw(error);
            expect(createWorker({})).to.throw(error);
        });

        it('should throw an error if a valid retry monitor object is not specified', function() {
            var error = 'Invalid monitor specified (arg #2)';

            function createWorker(monitor) {
                return function() {
                    return new ParanoidPirateWorker(_testUtils.generateEndpoint(), monitor);
                };
            }

            expect(createWorker()).to.throw(error);
            expect(createWorker(null)).to.throw(error);
            expect(createWorker('')).to.throw(error);
            expect(createWorker(1)).to.throw(error);
            expect(createWorker(true)).to.throw(error);
            expect(createWorker([])).to.throw(error);
            expect(createWorker({})).to.throw(error);
        });

        it('should create an object that exposes members required by the interface', function() {
            var monitor = new Monitor(2500, 3);
            var worker = new ParanoidPirateWorker(_testUtils.generateEndpoint(), monitor);

            expect(worker).to.be.an('object');
            expect(worker).to.be.an.instanceof(_events.EventEmitter);
            expect(worker).to.have.property('initialize').and.to.be.a('function');
            expect(worker).to.have.property('send').and.to.be.a('function');
            expect(worker).to.have.property('dispose').and.to.be.a('function');
            expect(worker).to.have.property('isReady').and.to.be.a('function');
            expect(worker).to.have.property('getBackoffOptions').and.to.be.a('function');
        });

        it('should set property values to defaults', function() {
            var monitor = new Monitor(2500, 3);
            var worker = new ParanoidPirateWorker(_testUtils.generateEndpoint(), monitor);

            expect(worker.isReady()).to.be.false;
            var backoffOptions = worker.getBackoffOptions();
            expect(backoffOptions).to.be.an('object');
            expect(backoffOptions).to.have.property('backoff').and.to.be.a('number');
            expect(backoffOptions).to.have.property('retries').and.to.be.a('number');
        });

        it('should use input backoff options when specified during object creation', function() {
            var monitor = new Monitor(2500, 3);
            var opts = {
                backoff: 2512,
                retries: 87
            };
            var worker = new ParanoidPirateWorker(_testUtils.generateEndpoint(), monitor, opts);

            var backoffOptions = worker.getBackoffOptions();
            expect(backoffOptions).to.be.an('object');
            expect(backoffOptions).to.have.property('backoff').and.to.equal(opts.backoff);
            expect(backoffOptions).to.have.property('retries').and.to.equal(opts.retries);
        });
    });

    describe('getBackoffOptions()', function() {
        it('should return a copy of the backoff options', function() {
            var monitor = new Monitor(2500, 3);
            var opts = {
                backoff: 2512,
                retries: 87
            };
            var worker = new ParanoidPirateWorker(_testUtils.generateEndpoint(), monitor, opts);

            var backoffOptions = worker.getBackoffOptions();
            backoffOptions.backoff = 9999;
            backoffOptions.retries = 291;


            backoffOptions = worker.getBackoffOptions();
            expect(backoffOptions).to.be.an('object');
            expect(backoffOptions).to.have.property('backoff').and.to.equal(opts.backoff);
            expect(backoffOptions).to.have.property('retries').and.to.equal(opts.retries);
        });
    });

    describe('initialize()', function() {
        it('should initialize a connection to a peer endpoint when invoked', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();

            _worker = _createPPWorker(endpoint);
            _queue = _createQueue(endpoint);

            _queue.on('accept', function() {
                // If this event is not triggered, the test will timeout and fail.
                def.resolve();
            });
            expect(def.promise).to.be.fulfilled.notify(done);
            
            _worker.initialize();
        });

        it('should send a READY message to the queue when invoked', function(done){
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();

            _worker = _createPPWorker(endpoint);
            _queue = _createQueue(endpoint);

            _queue.on('message', function() {
                var frames = Array.prototype.splice.call(arguments, 0);

                _testUtils.runDeferred(function() {
                    expect(frames).to.have.length(2);
                    expect(frames[1].toString()).to.equal(_messageDefinitions.READY);
                }, def);
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);

        });

        it('should set isReady()=true once initialized', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();

            _worker = _createPPWorker(endpoint);
            _worker.on(_eventDefinitions.READY, function() {
                _testUtils.runDeferred(function() {
                    expect(_worker.isReady()).to.be.true;
                }, def);
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should raise a "ready" event with null args after initializing the object', function() {
            var handlerSpy = _sinon.spy();
            var endpoint = _testUtils.generateEndpoint();

            _worker = _createPPWorker(endpoint);
            _worker.on(_eventDefinitions.READY, handlerSpy);

            expect(handlerSpy).to.not.have.been.called;
            _worker.initialize();
            expect(handlerSpy).to.have.been.called;
            expect(handlerSpy).to.have.been.calledWithExactly(null);
        });
    });

    describe('send()', function() {
        it('should throw an error if invoked before the socket has been initialized', function() {
            var error = 'Socket not initialized. Cannot send message';
            var endpoint = _testUtils.generateEndpoint();

            _worker = _createPPWorker(endpoint);

            expect(function() { _worker.send('foo'); }).to.throw(error);
        });

        it('should send a message over a zero mq socket when invoked', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();
            var message = 'hello';

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint);

            _queue.on('message', function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                
                //We're going to get a READY message when the worker is
                //initialized. Ignore that and go to the next message.
                if(frames[1].toString() !== _messageDefinitions.READY) {
                    _testUtils.runDeferred(function(){
                        expect(frames).to.have.length(2);
                        expect(frames[1].toString()).to.equal(message);
                    }, def);
                } 
            });

            _worker.on(_eventDefinitions.READY, function() {
                _worker.send(message);
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should raise a "request" event once a request is received from a peer', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();
            var message1 = 'hello';
            var message2 = 'world';

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint);
            
            _queue.on('message', function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                if(frames[1].toString() === _messageDefinitions.READY) {
                    _queue.send([frames[0], 
                                _messageDefinitions.REQUEST, message1, message2]);
                }
            });

            _worker.on(_eventDefinitions.REQUEST, function(payload) {
                _testUtils.runDeferred(function() {
                    expect(payload).to.be.an('Array');
                    expect(payload).to.have.length(2);

                    expect(payload[0].toString()).to.equal(message1);
                    expect(payload[1].toString()).to.equal(message2);
                }, def);
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });
    });

    describe('dispose()', function() {
        it('should close an open socket, and set isReady()=false when invoked', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();
            var connectionMade = false;
            var wasReady = false;

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint);

            _queue.on('disconnect', function(message) {
                _testUtils.runDeferred(function() {

                    //Expect that the connection was live prior to the disconnect.
                    expect(connectionMade).to.be.true;

                    //Expect that the client was ready before disconnect, and that
                    //it is no longer ready after disconnect.
                    expect(wasReady).to.be.true;
                    expect(_worker.isReady()).to.be.false;
                }, def);
            });

            _worker.on(_eventDefinitions.READY, function() {
                connectionMade = true;
                wasReady = _worker.isReady();
                _worker.dispose();
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should terminate heartbeat monitoring when invoked', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();
            var heartBeatInterval = 100;
            var heartBeatCount = 0;

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint, new Monitor(heartBeatInterval, 10));

            _queue.on('message', function() {
                var frames = Array.prototype.splice.call(arguments, 0);

                var action = frames[1].toString();
                if(action === _messageDefinitions.HEARTBEAT) {
                    heartBeatCount++;
                    if(heartBeatCount === 3) {
                        _worker.dispose();
                        setTimeout(function() {
                            _testUtils.runDeferred(function() {
                                expect(heartBeatCount).to.equal(3);
                            }, def);
                        }, heartBeatInterval * 3);
                    }
                }
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });
    });

    describe('[HEARTBEAT LOGIC]', function() {
        var RETRY_FREQUENCY = 100;
        var RETRY_COUNT = 10;

        function _createMonitor(retryFrequency, retryCount) {
            retryFrequency = retryFrequency || RETRY_FREQUENCY;
            retryCount = retryCount || RETRY_COUNT;
            return new Monitor(retryFrequency, retryCount);
        }

        it('should send out a heartbeat message at a predetermined (monitor) frequency', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();

            var MAX_HEART_BEATS = 3;
            var receivedReady = false;
            var heartBeatCount = 0;
            var startTime = null;
            var aggregatePeriod = 0;

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint, _createMonitor());

            _queue.on('message', function() {
                var frames = Array.prototype.splice.call(arguments, 0);

                var action = frames[1].toString();
                switch(action) {
                    case _messageDefinitions.READY:
                        receivedReady = true;
                        break;
                    case _messageDefinitions.HEARTBEAT:
                        heartBeatCount++;
                        if(startTime !== null) {
                            var endTime = Date.now();
                            aggregatePeriod += (endTime - startTime);
                        }
                        startTime = Date.now();
                        if(heartBeatCount > MAX_HEART_BEATS) {
                            _testUtils.runDeferred(function() {
                                var averageHBFrequency = aggregatePeriod/MAX_HEART_BEATS;
                                var range = _getRange(RETRY_FREQUENCY, 5);
                                expect(receivedReady).to.be.true;
                                expect(averageHBFrequency).to.be.within(range[0], range[1]);
                            }, def);
                        }
                        break;
                }
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should close the socket if a heartbeat is not received for the specified number of cycles', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();

            var heartBeatCount = 0;
            var expectedHeartBeatCount = 3;

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint, _createMonitor(null, expectedHeartBeatCount));

            _queue.on('message', function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                var action = frames[1].toString();
                if(action === _messageDefinitions.HEARTBEAT) {
                    heartBeatCount++;
                }
            });

            _queue.on('disconnect', function() {
                _testUtils.runDeferred(function() {
                    expect(heartBeatCount).to.equal(expectedHeartBeatCount);
                }, def);
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should reopen a closed socket after an wait period', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();
            var backoffDuration = 10;

            var wasDisconnected = false;
            var startTime = null;
            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint, _createMonitor(10, 3), { backoff: backoffDuration });

            _queue.on('disconnect', function() {
                wasDisconnected = true;
                startTime = Date.now();
            });

            _queue.on('accept', function() {
                if(wasDisconnected) {
                    _testUtils.runDeferred(function() {
                        var timeTaken = Date.now() - startTime;
                        var range = _getRange(backoffDuration, 5);
                        expect(timeTaken).to.be.within(range[0], range[1]);
                    }, def);
                }
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should attempt to reopen a closed socket after exponentially increasing wait periods', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();
            // Has to be sufficiently large so that latency does not become
            // a significant factor in the math when computing backoffs.
            var backoffDuration = 100;
            var backoffRetries = 3;
            var heartbeatFrequency = 10;

            var wasDisconnected = false;
            var startTime = null;
            var retryDurations = [];

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint, _createMonitor(10, 3), {
                                            backoff: backoffDuration,
                                            retries: backoffRetries });

            _queue.on('disconnect', function() {
                wasDisconnected = true;
                startTime = Date.now();
            });

            _queue.on('accept', function() {
                if(wasDisconnected) {
                    retryDurations.push(Date.now() - startTime);
                    backoffRetries--;

                    if(backoffRetries === 0) {
                        _testUtils.runDeferred(function() {
                            var range = _getRange(backoffDuration, 15);
                            expect(retryDurations[0]).to.be.within(range[0], range[1]);

                            range = _getRange(backoffDuration * 2, 15);
                            expect(retryDurations[1]).to.be.within(range[0], range[1]);

                            range = _getRange(backoffDuration * 4, 15);
                            expect(retryDurations[2]).to.be.within(range[0], range[1]);
                        }, def);
                    }
                }
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should raise an "abandoned" event after retrying a specified number of times', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();
            var backoffDuration = 10;
            var backoffRetries = 3;
            var heartbeatFrequency = 10;

            var wasDisconnected = false;
            var startTime = null;

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint, _createMonitor(10, 3), {
                                            backoff: backoffDuration,
                                            retries: backoffRetries });

            _queue.on('disconnect', function() {
                wasDisconnected = true;
                startTime = Date.now();
            });

            _queue.on('accept', function() {
                if(wasDisconnected) {
                    backoffRetries--;
                }
            });

            _worker.on(_eventDefinitions.ABANDONED, function() {
                _testUtils.runDeferred(function() {
                    expect(backoffRetries).to.equal(0);
                }, def);
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should continue without closing as long as it receives heartbeats', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();

            var heartBeatCount = 0;
            var expectedHeartBeatCount = 3;
            var retryFrequency = 50;
            var disconnected = false;

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint, _createMonitor(retryFrequency, expectedHeartBeatCount));

            _queue.on('message', function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                var action = frames[1].toString();
                if(action === _messageDefinitions.HEARTBEAT) {
                    heartBeatCount++;
                }
                if(heartBeatCount > expectedHeartBeatCount * 2) {
                    _testUtils.runDeferred(function() {
                        expect(disconnected).to.be.false;
                    }, def);
                }
                _queue.send([frames[0], _messageDefinitions.HEARTBEAT]);
            });

            _queue.on('disconnect', function() {
                disconnected = true;
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });
    })
});
