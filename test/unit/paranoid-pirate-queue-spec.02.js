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

    describe('[HEARTBEAT/EXPIRY LOGIC]', function() {

        it('should store worker metadata when a new worker makes a connection', function(done) {
            var workerCount = 3;
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(null, beEndpoint);

            var doTests = function() {
                var knownWorkers = _queue.getWorkerMap();
                var now = Date.now();

                expect(knownWorkers).to.not.be.empty;
                for(var workerId in knownWorkers) {
                    var worker = knownWorkers[workerId];
                    
                    expect(worker).to.be.an('object');
                    expect(worker).to.have.property('address').and.to.be.an('object');
                    expect(worker).to.have.property('id', workerId);
                    expect(worker).to.have.property('lastAccess').and.to.be.within(now-50, now+50);
                }
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait(10))

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should update worker timestamp when the worker sends a heartbeat', function(done) {
            var workerCount = 3;
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var initialWorkerMap = null;
            var saveInitialWorkerMap = function(workers) {
                initialWorkerMap = _queue.getWorkerMap();
                return workers;
            };

            var doTests = function() {
                var workerMap = _queue.getWorkerMap();

                expect(workerMap).to.not.be.empty;
                for(var workerId in workerMap) {
                    var workerInfo = workerMap[workerId];
                    var initialWorkerInfo = initialWorkerMap[workerId];
                        
                    expect(initialWorkerInfo.address).to.deep.equal(workerInfo.address);
                    expect(initialWorkerInfo.lastAccess).to.be.below(workerInfo.lastAccess);
                }
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(saveInitialWorkerMap)
                .then(_queueUtil.sendMessages(_messageDefinitions.HEARTBEAT))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should update worker timestamp when the worker replies to a request', function(done) {
            var workerCount = 3;
            var clientCount = 3;
            var workerResponse = 'OK';
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            var initialWorkerMap = null;
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var def = _q.defer();
            var responseCount = workerCount;
            _queue.on(_eventDefinitions.ASSIGNED_RESPONSE, function() {
                responseCount--;
                if(responseCount === 0) {
                    // Run tests after both workers have responded.
                    def.resolve();
                }
            });

            var workerMessageHandler = function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                this.send([_messageDefinitions.RESPONSE, frames[1], frames[2], workerResponse]);
            };

            var saveInitialWorkerMap = function(workers) {
                initialWorkerMap = _queue.getWorkerMap();
                return workers;
            };

            var doTests = function() {
                var workerMap = _queue.getWorkerMap();

                expect(workerMap).to.not.be.empty;
                for(var workerId in workerMap) {
                    var workerInfo = workerMap[workerId];
                    var initialWorkerInfo = initialWorkerMap[workerId];
                        
                    expect(initialWorkerInfo.address).to.deep.equal(workerInfo.address);
                    expect(initialWorkerInfo.lastAccess).to.be.below(workerInfo.lastAccess);
                }
            };

            var message1 = _queueUtil.createClientRequest('', 'message#1');
            var message2 = _queueUtil.createClientRequest('', 'message#2');

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMessageHandler))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(saveInitialWorkerMap)
                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.sendMessages(message1, message2))
                .then(_queueUtil.waitForResolution(def))

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should respond to a heartbeat with another heartbeat', function(done) {
            var workerCount = 2;
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var frameSets = [];
            var def = _q.defer();
            var responseCount = workerCount;
            var workerMessageHandler = function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                frameSets.push(frames);

                responseCount--;
                if(responseCount === 0) {
                    def.resolve();
                }
            };

            var doTests = function(workers) {
                frameSets.forEach(function(frames) {
                    expect(frames).to.have.length(1);
                    expect(frames[0].toString()).to.equal(_messageDefinitions.HEARTBEAT);
                });
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMessageHandler))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.sendMessages(_messageDefinitions.HEARTBEAT))
                .then(_queueUtil.waitForResolution(def))

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should not respond to messages with invalid action headers', function(done) {
            var workerCount = 3;
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(null, beEndpoint);

            var responseCount = 0;
            var workerMessageHandler = function() {
                responseCount++;
            };

            var doTests = function() {
                expect(responseCount).to.equal(0);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMessageHandler))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.sendMessages('BAD ACTION'))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should not respond to heartbeats from workers that have not registered with the queue', function(done) {
            var workerCount = 3;
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(null, beEndpoint);

            var responseCount = 0;
            var workerMessageHandler = function() {
                responseCount++;
            };

            var doTests = function() {
                expect(responseCount).to.equal(0);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMessageHandler))
                .then(_queueUtil.sendMessages(_messageDefinitions.HEARTBEAT))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should not add a worker sending a heartbeat to the available workers list', function(done) {
            var workerCount = 3;
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(null, beEndpoint);

            var doTests = function() {
                expect(_queue.getAvailableWorkerCount()).to.equal(workerCount);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.sendMessages(_messageDefinitions.HEARTBEAT))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should not mark a worker as available when a heartbeat is received', function(done) {
            var workerCount = 3;
            var clientCount = 3;
            var clientMessage = _queueUtil.createClientRequest();
            var beEndpoint = _testUtil.generateEndpoint();
            var feEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var doTests = function(sockets) {
                var map = _queue.getWorkerMap();

                for(var workerId in map) {
                    var worker = map[workerId];
                    expect(worker.isAvailable).to.be.false;
                }
                return sockets;
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.captureContext('worker'))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                .then(_queueUtil.switchContext('worker'))
                .then(doTests)

                .then(_queueUtil.sendMessages(_messageDefinitions.HEARTBEAT))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should not send a pending request to a worker sending a heartbeat', function(done) {
            var workerCount = 3;
            var clientCount = 4;
            var clientMessage = _queueUtil.createClientRequest();
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var doTests = function() {
                expect(_queue.getPendingRequestCount()).to.equal(clientCount - workerCount);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.sendMessages(_messageDefinitions.HEARTBEAT))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should discard a worker if workerTimeout expires since last contact with the worker', function(done) {
            var workerCount = 3;
            var pollFrequency = 100;
            var workerTimeout = 300;
            var beEndpoint = _testUtil.generateEndpoint();

            _queue = _queueUtil.createPPQueue(null, beEndpoint, {
                pollFrequency: pollFrequency,
                workerTimeout: workerTimeout
            });

            var verifyWorkerCount = function() {
                var workerMap = _queue.getWorkerMap();

                expect(_queue.getAvailableWorkerCount()).to.equal(workerCount);
                expect(Object.keys(workerMap)).to.have.length(workerCount);
            };

            var doTests = function() {
                var workerMap = _queue.getWorkerMap();

                expect(_queue.getAvailableWorkerCount()).to.equal(0);
                expect(workerMap).to.be.empty;
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(verifyWorkerCount)
                .then(_queueUtil.wait(pollFrequency * 4))

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should not send a request to a worker whose timeout has expired', function(done) {
            var clientCount = 3;
            var workerCount = 3;
            var clientMessage = _queueUtil.createClientRequest();
            var pollFrequency = 100;
            var workerTimeout = 300;
            var expiredWorkerHandler = _sinon.spy();
            var validWorkerHandler = _sinon.spy();
            var beEndpoint = _testUtil.generateEndpoint();
            var feEndpoint = _testUtil.generateEndpoint();

            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint, {
                pollFrequency: pollFrequency,
                workerTimeout: workerTimeout
            });

            var verifyWorkerCount = function() {
                var workerMap = _queue.getWorkerMap();

                expect(_queue.getAvailableWorkerCount()).to.equal(0);
                expect(Object.keys(workerMap)).to.have.length(0);
            };

            var doTests = function() {
                expect(expiredWorkerHandler).not.to.have.not.been.called;
                expect(validWorkerHandler).to.have.been.calledThrice;
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint, true))
                .then(_queueUtil.setupHandlers('message', expiredWorkerHandler))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait(pollFrequency * 4))

                .then(verifyWorkerCount)

                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.setupHandlers('message', validWorkerHandler))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('req', workerCount, feEndpoint))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });
    });

    describe('[SESSION LOGIC]', function() {

        it('should distribute requests from one client to multiple workers when session has not been enabled', function(done) {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint, {
                pollFrequency: 5000,
                workerTimeout: 10000
            });

            var clientIds = [ 'client1', 'client2' ];
            var requests = _queueUtil.createClientMessages(clientIds);

            var workerMap = _queueUtil.createWorkerMap(['worker1', 'worker2']);

            var doTests = function() {
                workerMap._workerList.forEach(function(worker) {
                    expect(worker.messages).to.include.members(clientIds);
                });
            };

            var sendMessagesInReverseOrder = function(sockets) {
                sockets[1].send(requests[1]);
                sockets[0].send(requests[0]);
                return sockets;
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('req', clientIds.length, feEndpoint))
                .then(_queueUtil.sendMessages(requests[0], requests[1]))
                .then(_queueUtil.wait())

                // Send messages in reverse order, so that they are distributed across workers.
                .then(sendMessagesInReverseOrder)
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should create a new session, and add the client request to the pending request queue if no workers are available', function(done) {
            var clientCount = 3;
            var clientMessage = _queueUtil.createClientRequest();
            var feEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, null, {
                pollFrequency: 5000,
                workerTimeout: 10000,
                session: {
                    timeout: 30000
                }
            });

            var now = Date.now();
            var doTests = function() {
                var sessionMap = _queue.getSessionMap();
                expect(Object.keys(sessionMap)).to.have.length(clientCount);

                for(var sessionId in sessionMap) {
                    var session = sessionMap[sessionId];
                    expect(session.id).to.be.a('string').and.not.to.be.empty;
                    expect(session.lastAccess).to.be.above(now);
                    expect(session.workerId).to.be.a('string').and.to.be.empty;
                }
                expect(_queue.getPendingRequestCount()).to.equal(clientCount);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should create a new session and assign a new client a free worker if one is available', function(done) {
            var workerCount = 3;
            var clientCount = 3;
            var clientMessage = _queueUtil.createClientRequest();
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint, {
                pollFrequency: 5000,
                workerTimeout: 10000,
                session: {
                    timeout: 30000
                }
            });

            var now = Date.now();
            var doTests = function() {
                var workerMap = _queue.getWorkerMap();
                var sessionMap = _queue.getSessionMap();
                expect(Object.keys(sessionMap)).to.have.length(clientCount);

                for(var sessionId in sessionMap) {
                    var session = sessionMap[sessionId];
                    expect(session.id).to.be.a('string').and.not.to.be.empty;
                    expect(session.lastAccess).to.be.above(now);
                    expect(session.workerId).to.be.a('string').and.not.to.be.empty;
                    expect(workerMap).to.contain.keys(session.workerId);
                }
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should assign a worker id to an unassigned client session once a new worker becomes available', function(done) {
            var workerCount = 3;
            var clientCount = 3;
            var clientMessage = _queueUtil.createClientRequest();
            var beEndpoint = _testUtil.generateEndpoint();
            var feEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint, {
                pollFrequency: 5000,
                workerTimeout: 10000,
                session: {
                    timeout: 30000
                }
            });

            var now = Date.now();
            var testForUnassignedSessions = function() {
                var sessionMap = _queue.getSessionMap();
                expect(Object.keys(sessionMap)).to.have.length(clientCount);

                for(var sessionId in sessionMap) {
                    var session = sessionMap[sessionId];
                    expect(session.id).to.be.a('string').and.not.to.be.empty;
                    expect(session.lastAccess).to.be.above(now);
                    expect(session.workerId).to.be.a('string').and.to.be.empty;
                }
                expect(_queue.getPendingRequestCount()).to.equal(clientCount);
            };

            var testForAssignedSessions = function() {
                var workerMap = _queue.getWorkerMap();
                var sessionMap = _queue.getSessionMap();
                expect(Object.keys(sessionMap)).to.have.length(clientCount);

                for(var sessionId in sessionMap) {
                    var session = sessionMap[sessionId];
                    expect(session.id).to.be.a('string').and.not.to.be.empty;
                    expect(session.lastAccess).to.be.above(now);
                    expect(session.workerId).to.be.a('string').and.not.to.be.empty;
                    expect(workerMap).to.contain.keys(session.workerId);
                }
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                .then(testForUnassignedSessions)

                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(testForAssignedSessions)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should route all requests based on established session affinity (workers connect first)', function(done) {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint, {
                pollFrequency: 5000,
                workerTimeout: 10000,
                session: {
                    timeout: 30000
                }
            });

            var workerIds = [ 'worker1', 'worker2' ];
            var clientIds = [ 'client1', 'client2', 'client3', 'client4' ];
            var requests = _queueUtil.createClientMessages(clientIds);
            var workerMap = _queueUtil.createWorkerMap(workerIds);

            var doTests = function() {
                var workerList = workerMap._workerList;
                
                for(var index=0; index<workerIds.length; index++) {
                    expect(workerList[index].messages).to.deep.equal([
                        clientIds[index], clientIds[index + 2],
                        clientIds[index + 2], clientIds[index]
                    ]);
                }
            };

            var sendMessagesInReverseOrder = function(sockets) {
                for(var index=clientIds.length-1; index>=0; index--) {
                    sockets[index].send(requests[index]);
                }
                return sockets;
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('req', clientIds.length, feEndpoint))
                .then(_queueUtil.sendMessages.apply(_queueUtil, requests))
                .then(_queueUtil.wait())

                // Send messages in reverse order, but expect session affinity to be honored
                .then(sendMessagesInReverseOrder)
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should route all requests based on established session affinity (clients connect first)', function(done) {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint, {
                pollFrequency: 5000,
                workerTimeout: 10000,
                session: {
                    timeout: 30000
                }
            });

            var workerIds = [ 'worker1', 'worker2' ];
            var clientIds = [ 'client1', 'client2', 'client3', 'client4' ];
            var requests = _queueUtil.createClientMessages(clientIds, null, true);
            var workerMap = _queueUtil.createWorkerMap(workerIds);

            var doTests = function() {
                var workerList = workerMap._workerList;
                
                for(var index=0; index<workerIds.length; index++) {
                    expect(workerList[index].messages).to.deep.equal([
                        clientIds[index], clientIds[index + 2],
                        clientIds[index + 2], clientIds[index]
                    ]);
                }
            };

            var sendMessagesInReverseOrder = function(sockets) {
                for(var index=clientIds.length-1; index>=0; index--) {
                    sockets[index].send(requests[index]);
                }
                return sockets;
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', clientIds.length, feEndpoint))
                .then(_queueUtil.sendMessages.apply(_queueUtil, requests))
                .then(_queueUtil.wait())

                // Send messages in reverse order, but expect session affinity to be honored
                .then(sendMessagesInReverseOrder)
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should process requests in order, without giving preferential treatment to clients with sessions', function(done) {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            var clientMessage = _queueUtil.createClientRequest();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint, {
                pollFrequency: 5000,
                workerTimeout: 10000,
                session: {
                    timeout: 30000
                }
            });

            var workerIds = [ 'worker1' ];
            var clientIds = [ 'client1', 'client2', 'client3' ];
            var requests = _queueUtil.createClientMessages(clientIds);
            requests[2].push('block');

            var workerMap = _queueUtil.createWorkerMap(workerIds);

            var doTests = function() {
                var workerList = workerMap._workerList;

                expect(workerList[0].messages).to.deep.equal([
                    clientIds[0], clientIds[1], clientIds[2],
                    clientMessage[2], clientMessage[2], clientMessage[2],
                    clientIds[0], clientIds[1], clientIds[2]
                ]);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('req', clientIds.length, feEndpoint))
                .then(_queueUtil.captureContext('client'))
                .then(_queueUtil.sendMessages(requests[0], requests[1], requests[2]))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint, true))
                .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                //Create dummy clients whose messages should be processed in order
                .then(_queueUtil.initSockets('req', 3, feEndpoint))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                //Send some requests to get queued.
                .then(_queueUtil.switchContext('client'))
                .then(_queueUtil.sendMessages(requests[0], requests[1], requests[2]))
                .then(_queueUtil.wait())

                //Unblock the workers
                .then(workerMap.getUnblockAll())
                .then(_queueUtil.wait(50))

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should assign a request to the session defined worker, even if the worker has died', function(done) {
            var clientCount = 3;
            var clientMessage = _queueUtil.createClientRequest();
            var pollFrequency = 100;
            var workerTimeout = 300;
            var beEndpoint = _testUtil.generateEndpoint();
            var feEndpoint = _testUtil.generateEndpoint();

            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint, {
                pollFrequency: pollFrequency,
                workerTimeout: workerTimeout,
                session: {
                    timeout: 30000
                }
            });

            var workerIds = [ 'worker1', 'worker2', 'worker3' ];
            var firstWorkerMap = _queueUtil.createWorkerMap(workerIds);
            var secondWorkerMap = _queueUtil.createWorkerMap(workerIds);

            var counts = {
                first: 0,
                second: 0
            };

            function createMessageHandler(setName) {
                return function() {
                    counts[setName]++;
                };
            }

            var doTests = function() {
                expect(_queue.getPendingRequestCount()).to.equal(clientCount);
                expect(counts.first).to.equal(clientCount);
                expect(counts.second).to.equal(clientCount);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', firstWorkerMap._count, beEndpoint, true))
                .then(_queueUtil.setupHandlers('message', firstWorkerMap._handlers))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.captureContext('client'))
                .then(_queueUtil.setupHandlers('message', createMessageHandler('first')))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait(pollFrequency * 4))

                // Create a second set of workers and clients, but the new workers 
                // should not touch messages from the previous clients, even though
                // the workers for those clients have died.
                .then(_queueUtil.initSockets('dealer', secondWorkerMap._count, beEndpoint, true))
                .then(_queueUtil.setupHandlers('message', secondWorkerMap._handlers))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.switchContext('client'))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.setupHandlers('message', createMessageHandler('second')))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should show the correct number of pending requests when a request is assigned to a worker', function(done) {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint, {
                pollFrequency: 5000,
                workerTimeout: 10000,
                session: {
                    timeout: 30000
                }
            });

            var workerIds = [ 'worker1' ];
            var clientIds = [ 'client1', 'client2', 'client3' ];
            var requests = _queueUtil.createClientMessages(clientIds);
            requests[2].push('block');

            var workerMap = _queueUtil.createWorkerMap(workerIds);

            var doTests = function() {
                var workerInfo = _queue.getWorkerMap();
                var workerId = Object.keys(workerInfo)[0];
                var worker = workerInfo[workerId];

                expect(worker.pendingRequestCount).to.equal(clientIds.length - 1);
                expect(_queue.getPendingRequestCount()).to.equal(clientIds.length - 1);
            };

            var checkStateAndSendMessages = function(sockets) {
                var workerInfo = _queue.getWorkerMap();
                var workerId = Object.keys(workerInfo)[0];
                var worker = workerInfo[workerId];

                expect(worker.pendingRequestCount).to.equal(0);
                expect(_queue.getPendingRequestCount()).to.equal(0);

                sockets[1].send(requests[1]);
                sockets[0].send(requests[0]);
                return sockets;
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint, true))
                .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('req', clientIds.length, feEndpoint))
                .then(_queueUtil.sendMessages.apply(_queueUtil, requests))
                .then(_queueUtil.wait())

                // Send messages for clients #1, and #2 to build up pending message
                // count
                .then(checkStateAndSendMessages)
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should show the correct number of pending requests when a worker picks up an enqueued request', function(done) {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint, {
                pollFrequency: 5000,
                workerTimeout: 10000,
                session: {
                    timeout: 30000
                }
            });

            var workerIds = [ 'worker1' ];
            var clientIds = [ 'client1', 'client2', 'client3' ];
            var requests = _queueUtil.createClientMessages(clientIds);
            requests[2].push('block');

            var workerMap = _queueUtil.createWorkerMap(workerIds);

            var doTests = function() {
                var workerInfo = _queue.getWorkerMap();
                var workerId = Object.keys(workerInfo)[0];
                var worker = workerInfo[workerId];

                expect(worker.pendingRequestCount).to.equal(0);
                expect(_queue.getPendingRequestCount()).to.equal(0);
            };

            var checkStateAndSendMessages = function(sockets) {
                var workerInfo = _queue.getWorkerMap();
                var workerId = Object.keys(workerInfo)[0];
                var worker = workerInfo[workerId];

                expect(worker.pendingRequestCount).to.equal(0);
                expect(_queue.getPendingRequestCount()).to.equal(0);

                sockets[1].send(requests[1]);
                sockets[0].send(requests[0]);
                return sockets;
            };

            var checkRequestCount = function() {
                var workerInfo = _queue.getWorkerMap();
                var workerId = Object.keys(workerInfo)[0];
                var worker = workerInfo[workerId];

                expect(_queue.getPendingRequestCount()).to.equal(clientIds.length - 1);
                expect(worker.pendingRequestCount).to.equal(clientIds.length - 1);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint, true))
                .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('req', clientIds.length, feEndpoint))
                .then(_queueUtil.sendMessages.apply(_queueUtil, requests))
                .then(_queueUtil.wait())

                .then(checkStateAndSendMessages)
                .then(_queueUtil.wait())

                .then(checkRequestCount)
                .then(workerMap.getUnblockAll())
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should show the correct number of available workers when a session bound client sends a new request', function(done) {
            var clientCount = 3;
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint, {
                pollFrequency: 5000,
                workerTimeout: 10000,
                session: {
                    timeout: 30000
                }
            });

            var workerIds = [ 'worker1', 'worker2', 'worker3' ];
            var clientIds = [ 'client1', 'client2', 'client3' ];
            var requests = _queueUtil.createClientMessages(clientIds);
            requests.forEach(function(request) {
                request.push('block');
            });

            var workerMap = _queueUtil.createWorkerMap(workerIds);

            function checkAvailableWorkers(count) {
                return function(data) {
                    expect(_queue.getAvailableWorkerCount()).to.equal(count);
                    return data;
                };
            }

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint))
                .then(_queueUtil.captureContext('worker'))
                .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.captureContext('client'))
                .then(_queueUtil.sendMessages.apply(_queueUtil, requests))
                .then(_queueUtil.wait())

                .then(checkAvailableWorkers(0))
                .then(_queueUtil.switchContext('worker'))
                .then(workerMap.getUnblockAll())
                .then(_queueUtil.wait())

                .then(checkAvailableWorkers(workerIds.length))

                .then(_queueUtil.switchContext('client'))
                .then(_queueUtil.sendMessages.apply(_queueUtil, requests))
                .then(_queueUtil.wait())

                .then(checkAvailableWorkers(0))
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should update session timestamp when a client sends a request', function(done) {
            var clientCount = 3;
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            var clientMessage = _queueUtil.createClientRequest();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint, {
                pollFrequency: 5000,
                workerTimeout: 10000,
                session: {
                    timeout: 30000
                }
            });

            var workerIds = [ 'worker1' ];
            var workerMap = _queueUtil.createWorkerMap(workerIds);

            var initialSessionMap = null;
            var saveInitialSessionMap = function(sockets) {
                initialSessionMap = _queue.getSessionMap();
                return sockets;
            };

            var doTests = function() {
                var sessionMap = _queue.getSessionMap();

                expect(sessionMap).to.not.be.empty;
                for(var sessionId in sessionMap) {
                    var session = sessionMap[sessionId];
                    var initialSession = initialSessionMap[sessionId];
                        
                    expect(initialSession.id).to.deep.equal(session.id);
                    expect(initialSession.lastAccess).to.be.below(session.lastAccess);
                }
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                .then(saveInitialSessionMap)
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should discard the client session once the session timeout expires', function(done) {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            var clientCount = 3;
            var workerCount = 3;
            var clientMessage = _queueUtil.createClientRequest();
            var sessionTimeout = 500;
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint, {
                pollFrequency: sessionTimeout/2,
                workerTimeout: 10000,
                session: {
                    timeout: sessionTimeout
                }
            });

            var checkForValidSession = function() {
                var sessionMap = _queue.getSessionMap();

                expect(Object.keys(sessionMap)).to.have.length(clientCount);
                for(var sessionId in sessionMap) {
                    var session = sessionMap[sessionId];

                    expect(session).to.be.an('object');
                }
            };

            var doTests = function() {
                var sessionMap = _queue.getSessionMap();

                expect(sessionMap).to.be.empty;
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint, true))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.captureContext('client'))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                .then(checkForValidSession)
                .then(_queueUtil.wait(sessionTimeout*1.5))

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });
    });
});
