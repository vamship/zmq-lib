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
var _testUtil = require('../test-util');
var Monitor = require('../../lib/monitor');
var ParanoidPirateWorker = require('../../lib/paranoid-pirate-worker');
var _messageDefinitions = require('../../lib/message-definitions');
var _eventDefinitions = require('../../lib/event-definitions');

describe('ParanoidPirateWorker', function(){
    var DEFAULT_RETRY_FREQ = 2500;
    var DEFAULT_RETRY_COUNT = 3;
    var DEFAULT_WORKER_OPTIONS = { backoff: 10, retries: 3, idleTimeout: -1 };
    var _queue;
    var _worker;

    function _createPPWorker(endpoint, monitor, workerOptions) {
        workerOptions = workerOptions || DEFAULT_WORKER_OPTIONS;
        monitor = monitor || new Monitor(DEFAULT_RETRY_FREQ, DEFAULT_RETRY_COUNT)
        var worker = new ParanoidPirateWorker(endpoint, monitor, workerOptions);

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
                    return new ParanoidPirateWorker(_testUtil.generateEndpoint(), monitor);
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
            var worker = new ParanoidPirateWorker(_testUtil.generateEndpoint(), monitor);

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
            var worker = new ParanoidPirateWorker(_testUtil.generateEndpoint(), monitor);

            expect(worker.isReady()).to.be.false;
            var workerOptions = worker.getBackoffOptions();
            expect(workerOptions).to.be.an('object');
            expect(workerOptions).to.have.property('backoff').and.to.be.a('number');
            expect(workerOptions).to.have.property('retries').and.to.be.a('number');
        });

        it('should use input backoff options when specified during object creation', function() {
            var monitor = new Monitor(2500, 3);
            var opts = {
                backoff: 2512,
                retries: 87
            };
            var worker = new ParanoidPirateWorker(_testUtil.generateEndpoint(), monitor, opts);

            var workerOptions = worker.getBackoffOptions();
            expect(workerOptions).to.be.an('object');
            expect(workerOptions).to.have.property('backoff').and.to.equal(opts.backoff);
            expect(workerOptions).to.have.property('retries').and.to.equal(opts.retries);
        });
    });

    describe('getBackoffOptions()', function() {
        it('should return a copy of the backoff options', function() {
            var monitor = new Monitor(2500, 3);
            var opts = {
                backoff: 2512,
                retries: 87
            };
            var worker = new ParanoidPirateWorker(_testUtil.generateEndpoint(), monitor, opts);

            var workerOptions = worker.getBackoffOptions();
            workerOptions.backoff = 9999;
            workerOptions.retries = 291;


            workerOptions = worker.getBackoffOptions();
            expect(workerOptions).to.be.an('object');
            expect(workerOptions).to.have.property('backoff').and.to.equal(opts.backoff);
            expect(workerOptions).to.have.property('retries').and.to.equal(opts.retries);
        });
    });

    describe('initialize()', function() {
        it('should initialize a connection to a peer endpoint when invoked', function(done) {
            var def = _q.defer();
            var endpoint = _testUtil.generateEndpoint();

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
            var endpoint = _testUtil.generateEndpoint();

            _worker = _createPPWorker(endpoint);
            _queue = _createQueue(endpoint);

            _queue.on('message', function() {
                var frames = Array.prototype.splice.call(arguments, 0);

                _testUtil.runDeferred(function() {
                    expect(frames).to.have.length(2);
                    expect(frames[1].toString()).to.equal(_messageDefinitions.READY);
                }, def);
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);

        });

        it('should send a READY message with service preferences to the queue when invoked', function(done){
            var def = _q.defer();
            var endpoint = _testUtil.generateEndpoint();
            var services = [ 'service1', 'service2', 'service3' ];

            _worker = _createPPWorker(endpoint);
            _queue = _createQueue(endpoint);

            _queue.on('message', function() {
                var frames = Array.prototype.splice.call(arguments, 0);

                _testUtil.runDeferred(function() {
                    expect(frames).to.have.length(services.length + 2);
                    expect(frames[1].toString()).to.equal(_messageDefinitions.READY);
                    var index=2;
                    services.forEach(function(service) {
                        expect(service).to.equal(frames[index].toString());
                        index++;
                    });
                }, def);
            });

            _worker.initialize.apply(_worker, services);

            expect(def.promise).to.be.fulfilled.notify(done);

        });

        it('should set isReady()=true once initialized', function(done) {
            var def = _q.defer();
            var endpoint = _testUtil.generateEndpoint();

            _worker = _createPPWorker(endpoint);
            _worker.on(_eventDefinitions.READY, function() {
                _testUtil.runDeferred(function() {
                    expect(_worker.isReady()).to.be.true;
                }, def);
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should raise a "ready" event with null args after initializing the object', function() {
            var handlerSpy = _sinon.spy();
            var endpoint = _testUtil.generateEndpoint();

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
            var endpoint = _testUtil.generateEndpoint();

            _worker = _createPPWorker(endpoint);

            expect(function() { _worker.send('foo'); }).to.throw(error);
        });

        it('should send a message over a zero mq socket when invoked', function(done) {
            var def = _q.defer();
            var endpoint = _testUtil.generateEndpoint();
            var message = 'hello';

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint);

            _queue.on('message', function() {
                var frames = Array.prototype.splice.call(arguments, 0);

                //We're going to get a READY message when the worker is
                //initialized. Ignore that and go to the next message.
                if(frames[1].toString() !== _messageDefinitions.READY) {
                    _testUtil.runDeferred(function(){
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

        it('should send a an array message as a multipart zero mq message', function(done) {
            var def = _q.defer();
            var endpoint = _testUtil.generateEndpoint();
            var message = ['hello', 'world'];

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint);

            _queue.on('message', function() {
                var frames = Array.prototype.splice.call(arguments, 0);

                //We're going to get a READY message when the worker is
                //initialized. Ignore that and go to the next message.
                if(frames[1].toString() !== _messageDefinitions.READY) {
                    _testUtil.runDeferred(function(){
                        expect(frames).to.have.length(3);
                        expect(frames[1].toString()).to.equal(message[0]);
                        expect(frames[2].toString()).to.equal(message[1]);
                    }, def);
                }
            });

            _worker.on(_eventDefinitions.READY, function() {
                _worker.send(message);
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

    });

    describe('dispose()', function() {
        it('should close an open socket, and set isReady()=false when invoked', function(done) {
            var def = _q.defer();
            var endpoint = _testUtil.generateEndpoint();
            var connectionMade = false;
            var wasReady = false;

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint);

            _queue.on('disconnect', function(message) {
                _testUtil.runDeferred(function() {

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
            var endpoint = _testUtil.generateEndpoint();
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
                            _testUtil.runDeferred(function() {
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
            var endpoint = _testUtil.generateEndpoint();

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
                            _testUtil.runDeferred(function() {
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
            var endpoint = _testUtil.generateEndpoint();

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
                _testUtil.runDeferred(function() {
                    expect(heartBeatCount).to.equal(expectedHeartBeatCount);
                }, def);
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should reopen a closed socket after an wait period', function(done) {
            var def = _q.defer();
            var endpoint = _testUtil.generateEndpoint();
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
                    _testUtil.runDeferred(function() {
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
            var endpoint = _testUtil.generateEndpoint();
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
                        _testUtil.runDeferred(function() {
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

        it('should continue without closing as long as it receives heartbeats', function(done) {
            var def = _q.defer();
            var endpoint = _testUtil.generateEndpoint();

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
                    _testUtil.runDeferred(function() {
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

    describe('[AUTO TERMINATE LOGIC]', function() {

        it('should auto terminate after an idle timeout duration, if one was specified during creation', function(done) {
            var def = _q.defer();
            var idleTimeout = 100;
            var endpoint = _testUtil.generateEndpoint();

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint, new Monitor(50, 3), {
                backoff: 10,
                retries: 3,
                idleTimeout: idleTimeout
            });

            _worker.on(_eventDefinitions.ABANDONED, function() {
                def.resolve();
            });

            _queue.on('message', function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                _queue.send([frames[0], _messageDefinitions.HEARTBEAT]);
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should ignore idle timeout if the worker is currently processing a request', function(done) {
            var idleTimeout = 100;
            var endpoint = _testUtil.generateEndpoint();
            var abandoned = false;

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint, new Monitor(50, 3), {
                backoff: 10,
                retries: 3,
                idleTimeout: idleTimeout
            });

            _worker.on(_eventDefinitions.ABANDONED, function() {
                abandoned = true;
            });

            _queue.on('message', function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                if(frames[1].toString() === _messageDefinitions.READY) {
                    _queue.send([frames[0],
                                _messageDefinitions.REQUEST]);
                } else {
                    _queue.send([frames[0], _messageDefinitions.HEARTBEAT]);
                }
            });

            var doTests = function() {
                expect(abandoned).to.be.false;
            };

            _worker.initialize();

            expect(_q.fcall(_testUtil.wait(idleTimeout * 4))).to.be.fulfilled
                .then(doTests)

                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should reset the idle timeout counter once the worker responds to the request', function(done) {
            var idleTimeout = 50;
            var endpoint = _testUtil.generateEndpoint();
            var abandoned = false;

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint, new Monitor(25, 3), {
                backoff: 10,
                retries: 3,
                idleTimeout: idleTimeout
            });

            _worker.on(_eventDefinitions.ABANDONED, function() {
                abandoned = true;
            });

            _queue.on('message', function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                if(frames[1].toString() === _messageDefinitions.READY) {
                    _queue.send([frames[0],
                                _messageDefinitions.REQUEST]);
                } else {
                    _queue.send([frames[0], _messageDefinitions.HEARTBEAT]);
                }
            });

            var respondToRequest = function() {
                expect(abandoned).to.be.false;
                _worker.send('DONE');
            };

            var doTests = function() {
                expect(abandoned).to.be.false;
            };

            _worker.initialize();

            expect(_q.fcall(_testUtil.wait(idleTimeout * 4))).to.be.fulfilled
                .then(respondToRequest)
                .then(_testUtil.wait(idleTimeout/2))
                .then(doTests)

                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should not restart the idle timeout counter if the send method is called with notDone=true', function(done) {
            var idleTimeout = 50;
            var endpoint = _testUtil.generateEndpoint();
            var abandoned = false;

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint, new Monitor(50, 3), {
                backoff: 10,
                retries: 3,
                idleTimeout: idleTimeout
            });

            _worker.on(_eventDefinitions.ABANDONED, function() {
                abandoned = true;
            });

            _queue.on('message', function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                if(frames[1].toString() === _messageDefinitions.READY) {
                    _queue.send([frames[0],
                                _messageDefinitions.REQUEST]);
                } else {
                    _queue.send([frames[0], _messageDefinitions.HEARTBEAT]);
                }
            });

            var respondToRequest = function() {
                expect(abandoned).to.be.false;
                _worker.send('DONE', true);
            };

            var doTests = function() {
                expect(abandoned).to.be.false;
            };

            _worker.initialize();

            expect(_q.fcall(_testUtil.wait(idleTimeout * 4))).to.be.fulfilled
                .then(respondToRequest)
                .then(_testUtil.wait(idleTimeout * 3))
                .then(doTests)

                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });
    });

    describe('[EVENTS]', function() {

        it('should emit the "REQUEST" event once a request is received from a peer', function(done) {
            var def = _q.defer();
            var endpoint = _testUtil.generateEndpoint();
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
                _testUtil.runDeferred(function() {
                    expect(payload).to.be.an('Array');
                    expect(payload).to.have.length(3);

                    expect(payload[0].toString()).to.equal(_messageDefinitions.REQUEST);
                    expect(payload[1].toString()).to.equal(message1);
                    expect(payload[2].toString()).to.equal(message2);
                }, def);
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should emit the "ABANDONED" event with the appropriate reason after retrying a specified number of times', function(done) {
            var def = _q.defer();
            var endpoint = _testUtil.generateEndpoint();
            var backoffDuration = 10;
            var backoffRetries = 3;
            var heartbeatFrequency = 10;
            var expectedReason = 'peer unreachable';

            var wasDisconnected = false;
            var startTime = null;

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint, new Monitor(10, 3), {
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

            _worker.on(_eventDefinitions.ABANDONED, function(reason) {
                _testUtil.runDeferred(function() {
                    expect(reason).to.equal(expectedReason);
                    expect(backoffRetries).to.equal(0);
                }, def);
            });

            _worker.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should emit the "ABANDONED" event with the appropriate reason, if the worker terminates due to idle timeout', function(done) {
            var idleTimeout = 100;
            var endpoint = _testUtil.generateEndpoint();
            var expectedReason = 'idle timeout expired';

            _queue = _createQueue(endpoint);
            _worker = _createPPWorker(endpoint, new Monitor(50, 3), {
                backoff: 10,
                retries: 3,
                idleTimeout: idleTimeout
            });

            _worker.on(_eventDefinitions.ABANDONED, function(reason) {
                expect(reason).to.equal(expectedReason);
                done();
            });

            _queue.on('message', function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                _queue.send([frames[0], _messageDefinitions.HEARTBEAT]);
            });

            _worker.initialize();
        });
    });
});
