/* jshint node:true, expr:true */
/* global xit: false */
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
var _queueUtil = require('../queue-util');

var ParanoidPirateQueue = require('../../lib/paranoid-pirate-queue');
var _messageDefinitions = require('../../lib/message-definitions');
var _eventDefinitions = require('../../lib/event-definitions');


describe('ParanoidPirateQueue', function() {
    var _queue;

    beforeEach(function() {
        _queueUtil.setup();
    })

    afterEach(function() {
        if(_queue) {
            _queue.dispose();
        }
        _queueUtil.teardown();
    });

    describe('[SERVICE BROKERING]', function() {

        it('should associate workers with any services specified as a part of the READY message', function(done){
            var workerMessages = [
                [ _messageDefinitions.READY, 'service1', 'service2' ],
                [ _messageDefinitions.READY, 'service2', 'service3' ],
                [ _messageDefinitions.READY ]
            ];
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(null, beEndpoint);

            var doTests = function(workerSockets) {
                var map = _queue.getWorkerMap();
                var workerIndex = 0;

                workerSockets.forEach(function(workerSocket) {
                    var workerId = (new Buffer(workerSocket.identity)).toString('base64');
                    var worker = map[workerId];
                    var expectedMessages = workerMessages[workerIndex];
                    workerIndex++;

                    expect(worker.isAvailable).to.be.true;
                    expect(worker.services).to.have.length(expectedMessages.length - 1);
                    for(var messageIndex=0; messageIndex<worker.services.length; messageIndex++) {
                        expect(expectedMessages[messageIndex + 1]).to.equal(worker.services[messageIndex]);
                    }
                });
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerMessages.length, beEndpoint, true))
                .then(_queueUtil.sendMessages.apply(_queueUtil, workerMessages))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should ignore empty service names specified during registration', function(done){
            var workerMessages = [
                [ _messageDefinitions.READY, 'service1', 'service2', '' ],
                [ _messageDefinitions.READY, 'service2', 'service3', '' ],
                [ _messageDefinitions.READY ]
            ];
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(null, beEndpoint);

            var doTests = function(workerSockets) {
                var map = _queue.getWorkerMap();
                var workerIndex = 0;

                workerSockets.forEach(function(workerSocket) {
                    var workerId = (new Buffer(workerSocket.identity)).toString('base64');
                    var worker = map[workerId];
                    var expectedMessages = workerMessages[workerIndex];
                    workerIndex++;

                    expect(worker.isAvailable).to.be.true;

                    var expectedCount = Math.max(expectedMessages.length - 2, 0);
                    expect(worker.services).to.have.length(expectedCount);
                    for(var messageIndex=0; messageIndex<worker.services.length; messageIndex++) {
                        expect(expectedMessages[messageIndex + 1]).to.equal(worker.services[messageIndex]);
                    }
                });
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerMessages.length, beEndpoint, true))
                .then(_queueUtil.sendMessages.apply(_queueUtil, workerMessages))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should deliver requests only to workers that can the request service type them (workers first)', function(done) {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var services = [ 'service1', 'service2' ]
            var clientIds = [ 'client1', 'client2' ];
            var requests = _queueUtil.createClientMessages(clientIds, services);

            var workerMap = _queueUtil.createWorkerMap(['worker1', 'worker2']);

            var doTests = function() {
                expect(workerMap._workerList[0].messages).to.deep.equal([ clientIds[1] ]);
                expect(workerMap._workerList[1].messages).to.deep.equal([ clientIds[0] ]);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                .then(_queueUtil.sendMessages( [ _messageDefinitions.READY, services[1] ],
                                               [ _messageDefinitions.READY, services[0] ]))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('req', clientIds.length, feEndpoint))
                .then(_queueUtil.sendMessages(requests[0], requests[1]))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should deliver requests only to workers that can support the request service type (clients first)', function(done) {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var services = [ 'service1', 'service2' ]
            var clientIds = [ 'client1', 'client2' ];
            var requests = _queueUtil.createClientMessages(clientIds, services);

            var workerMap = _queueUtil.createWorkerMap(['worker1', 'worker2']);

            var doTests = function() {
                expect(workerMap._workerList[0].messages).to.deep.equal([ clientIds[1] ]);
                expect(workerMap._workerList[1].messages).to.deep.equal([ clientIds[0] ]);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('req', clientIds.length, feEndpoint))
                .then(_queueUtil.sendMessages(requests[0], requests[1]))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                .then(_queueUtil.sendMessages( [ _messageDefinitions.READY, services[1] ],
                                               [ _messageDefinitions.READY, services[0] ]))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should deliver all types of requests to workers if the workers do not specify a service preference (workers first)', function(done) {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var services = [ 'service1', 'service2' ]
            var clientIds = [ 'client1', 'client2' ];
            var requests = _queueUtil.createClientMessages(clientIds, services);

            var workerMap = _queueUtil.createWorkerMap(['worker1']);

            var doTests = function() {
                expect(workerMap._workerList[0].messages).to.deep.equal(clientIds);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('req', clientIds.length, feEndpoint))
                .then(_queueUtil.sendMessages(requests[0], requests[1]))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should deliver all types of requests to workers if the workers do not specify a service preference (clients first)', function(done) {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var services = [ 'service1', 'service2' ]
            var clientIds = [ 'client1', 'client2' ];
            var requests = _queueUtil.createClientMessages(clientIds, services);

            var workerMap = _queueUtil.createWorkerMap(['worker1']);

            var doTests = function() {
                expect(workerMap._workerList[0].messages).to.deep.equal(clientIds);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('req', clientIds.length, feEndpoint))
                .then(_queueUtil.sendMessages(requests[0], requests[1]))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should deliver matching requests to workers registered with multiple request types (workers first)', function(done) {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var services = [ 'service1', 'service2' ]
            var clientIds = [ 'client1', 'client2' ];
            var requests = _queueUtil.createClientMessages(clientIds, services);

            var workerMap = _queueUtil.createWorkerMap(['worker1']);

            var doTests = function() {
                expect(workerMap._workerList[0].messages).to.deep.equal(clientIds);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                .then(_queueUtil.sendMessages([_messageDefinitions.READY].concat(services)))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('req', clientIds.length, feEndpoint))
                .then(_queueUtil.sendMessages(requests[0], requests[1]))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should deliver matching requests to workers registered with multiple request types (clients first)', function(done) {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var services = [ 'service1', 'service2' ]
            var clientIds = [ 'client1', 'client2' ];
            var requests = _queueUtil.createClientMessages(clientIds, services);

            var workerMap = _queueUtil.createWorkerMap(['worker1']);

            var doTests = function() {
                expect(workerMap._workerList[0].messages).to.deep.equal(clientIds);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('req', clientIds.length, feEndpoint))
                .then(_queueUtil.sendMessages(requests[0], requests[1]))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                .then(_queueUtil.sendMessages([_messageDefinitions.READY].concat(services)))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should match requests with empty service types only to workers registered with empty service types (workers first)', function(done) {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var clientIds = [ 'client1', 'client2' ];
            var requests = _queueUtil.createClientMessages(clientIds);

            var workerMap = _queueUtil.createWorkerMap(['worker1', 'worker2']);

            var doTests = function() {
                expect(workerMap._workerList[0].messages).to.deep.equal([ ]);
                expect(workerMap._workerList[1].messages).to.deep.equal(clientIds);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                .then(_queueUtil.sendMessages( [ _messageDefinitions.READY, 'service1' ],
                                               _messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('req', clientIds.length, feEndpoint))
                .then(_queueUtil.sendMessages(requests[0], requests[1]))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should match requests with empty service types only to workers registered with empty service types (clients first)', function(done) {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var clientIds = [ 'client1', 'client2' ];
            var requests = _queueUtil.createClientMessages(clientIds);

            var workerMap = _queueUtil.createWorkerMap(['worker1', 'worker2']);

            var doTests = function() {
                expect(workerMap._workerList[0].messages).to.deep.equal([ ]);
                expect(workerMap._workerList[1].messages).to.deep.equal(clientIds);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('req', clientIds.length, feEndpoint))
                .then(_queueUtil.sendMessages(requests[0], requests[1]))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                .then(_queueUtil.sendMessages( [ _messageDefinitions.READY, 'service1' ],
                                               _messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

    });
});
