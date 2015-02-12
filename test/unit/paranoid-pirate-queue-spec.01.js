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

    describe('ctor()', function() {
        it('should throw an error if invoked with an invalid front end endpoint', function() {
            var error = 'invalid front end endpoint specified (arg #1)';

            function createQueue(feEndpoint) {
                return function() {
                    return new ParanoidPirateQueue(feEndpoint);
                };
            }

            expect(createQueue()).to.throw(error);
            expect(createQueue(null)).to.throw(error);
            expect(createQueue('')).to.throw(error);
            expect(createQueue(1)).to.throw(error);
            expect(createQueue(true)).to.throw(error);
            expect(createQueue([])).to.throw(error);
            expect(createQueue({})).to.throw(error);
        });

        it('should throw an error if invoked with an invalid back end endpoint', function() {
            var feEndpoint = _testUtil.generateEndpoint();
            var error = 'invalid back end endpoint specified (arg #2)';

            function createQueue(beEndpoint) {
                return function() {
                    return new ParanoidPirateQueue(feEndpoint, beEndpoint);
                };
            }

            expect(createQueue()).to.throw(error);
            expect(createQueue(null)).to.throw(error);
            expect(createQueue('')).to.throw(error);
            expect(createQueue(1)).to.throw(error);
            expect(createQueue(true)).to.throw(error);
            expect(createQueue([])).to.throw(error);
            expect(createQueue({})).to.throw(error);
        });

        it('should throw an error if valid queue options are not specified', function() {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            var error = 'invalid queue options specified (arg #3)';

            function createQueue(options) {
                return function() {
                    return new ParanoidPirateQueue(feEndpoint, beEndpoint, options);
                };
            }
            expect(createQueue()).to.throw(error);
            expect(createQueue(null)).to.throw(error);
            expect(createQueue('abc')).to.throw(error);
            expect(createQueue(1)).to.throw(error);
            expect(createQueue(true)).to.throw(error);
        });

        it('should throw an error if the queue options does not include a poll frequency', function() {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            var error = 'queue options does not define a poll frequency property (queueOptions.pollFrequency)';

            function createQueue(pollFrequency) {
                var options = { pollFrequency: pollFrequency};
                return function() {
                    return new ParanoidPirateQueue(feEndpoint, beEndpoint, options);
                };
            }
            expect(createQueue()).to.throw(error);
            expect(createQueue(null)).to.throw(error);
            expect(createQueue('abc')).to.throw(error);
            expect(createQueue(0)).to.throw(error);
            expect(createQueue(-1)).to.throw(error);
            expect(createQueue(true)).to.throw(error);
            expect(createQueue([])).to.throw(error);
            expect(createQueue({})).to.throw(error);
        });

        it('should throw an error if the queue options does not include a worker timeout', function() {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            var error = 'queue options does not define a worker timeout property (queueOptions.workerTimeout)';

            function createQueue(workerTimeout) {
                var options = { pollFrequency: 1000, workerTimeout: workerTimeout};
                return function() {
                    return new ParanoidPirateQueue(feEndpoint, beEndpoint, options);
                };
            }
            expect(createQueue()).to.throw(error);
            expect(createQueue(null)).to.throw(error);
            expect(createQueue('abc')).to.throw(error);
            expect(createQueue(0)).to.throw(error);
            expect(createQueue(-1)).to.throw(error);
            expect(createQueue(true)).to.throw(error);
            expect(createQueue([])).to.throw(error);
            expect(createQueue({})).to.throw(error);
        });

        it('should throw an error if the queue options does not include a request timeout', function() {
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            var error = 'queue options does not define a request timeout property (queueOptions.requestTimeout)';

            function createQueue(requestTimeout) {
                var options = { pollFrequency: 1000, workerTimeout: 3000, requestTimeout: requestTimeout};
                return function() {
                    return new ParanoidPirateQueue(feEndpoint, beEndpoint, options);
                };
            }
            expect(createQueue()).to.throw(error);
            expect(createQueue(null)).to.throw(error);
            expect(createQueue('abc')).to.throw(error);
            expect(createQueue(0)).to.throw(error);
            expect(createQueue(-1)).to.throw(error);
            expect(createQueue(true)).to.throw(error);
            expect(createQueue([])).to.throw(error);
            expect(createQueue({})).to.throw(error);
        });

        it('should create an object that exposes members required by the interface', function() {
            _queue = _queueUtil.createPPQueue();

            expect(_queue).to.be.an.instanceof(_events.EventEmitter);
            expect(_queue).to.have.property('initialize').and.to.be.a('function');
            expect(_queue).to.have.property('dispose').and.to.be.a('function');
            expect(_queue).to.have.property('getPendingRequestCount').and.to.be.a('function');
            expect(_queue).to.have.property('getAvailableWorkerCount').and.to.be.a('function');
            expect(_queue).to.have.property('getWorkerMap').and.to.be.a('function');
            expect(_queue).to.have.property('getSessionMap').and.to.be.a('function');
            expect(_queue).to.have.property('isReady').and.to.be.a('function');
        });

        it('should initialize properties to default values', function() {
            _queue = _queueUtil.createPPQueue();

            expect(_queue.getPendingRequestCount()).to.equal(0);
            expect(_queue.getAvailableWorkerCount()).to.equal(0);
            expect(_queue.getWorkerMap()).to.deep.equal({});
            expect(_queue.getSessionMap()).to.deep.equal({});
            expect(_queue.isReady()).to.be.false;
        });
    });

    describe('initialize()', function() {
        it('should return a promise when invoked', function(){
            _queue = _queueUtil.createPPQueue();

            var ret = _queue.initialize();
            expect(ret).to.be.an('object');
            expect(ret).to.have.property('then').and.to.be.a('function');
        });

        it('should reject the promise if the front end binding fails', function(done) {
            _queue = _queueUtil.createPPQueue('bad-endpoint');
            
            expect(_queue.initialize()).to.be.rejected.notify(done);
        });

        it('should reject the promise if the back end binding fails', function(done){
            _queue = _queueUtil.createPPQueue(null, 'bad-endpoint');

            expect(_queue.initialize()).to.be.rejected.notify(done);
        });

        it('should reject the promise if both front and back end binrings fail', function(done){
            _queue = _queueUtil.createPPQueue('bad-endpoint', 'bad-endpoint');

            expect(_queue.initialize()).to.be.rejected.notify(done);
        });

        it('should resolve the promise once both front and back ends have been bound successfully', function(done){
            _queue = _queueUtil.createPPQueue();

            expect(_queue.initialize()).to.be.fulfilled.notify(done);
        });

        it('should set isReady()=true when initialization succeeds', function(done) {
            _queue = _queueUtil.createPPQueue();

            expect(_queue.isReady()).to.be.false;

            expect(_queue.initialize()).to.be.fulfilled.then(function() {
                expect(_queue.isReady()).to.be.true;
            }).then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should set isReady()=false when initialization fails', function(done) {
            _queue = _queueUtil.createPPQueue('bad-endpoint', 'bad-endpoint');

            expect(_queue.isReady()).to.be.false;

            expect(_queue.initialize()).to.be.rejected.then(function() {
                expect(_queue.isReady()).to.be.false;
            }).then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should open a front end socket and bind to the endpoint when invoked', function(done) {
            var feEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint);

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                var def = _q.defer();
                var client1 = _queueUtil.createReqSocket();

                client1.on('connect', function(){
                    def.resolve();
                });
                client1.connect(feEndpoint);

                return def.promise;
            }).then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should open a back end socket and bind to the endpoint when invoked', function(done) {
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(beEndpoint);

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                var def = _q.defer();
                var worker = _queueUtil.createDealerSocket();

                worker.on('connect', function(){
                    def.resolve();
                });
                worker.connect(beEndpoint);

                return def.promise;
            }).then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });
    });

    describe('getWorkerMap()', function() {
        it('should return an empty object when no workers have connected', function() {
            _queue = _queueUtil.createPPQueue();

            expect(_queue.getWorkerMap()).to.deep.equal({});
        });

        it('should return a copy of the map, and not a reference', function() {
            _queue = _queueUtil.createPPQueue();

            var map = _queue.getWorkerMap();
            expect(map).to.deep.equal({});
            expect(_queue.getWorkerMap()).not.to.equal(map);
        });

        it('should return a map with an entry for every worker that connects', function(done) {
            var workerCount = 10;
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(null, beEndpoint);

            var doTests = function() {
                var map = _queue.getWorkerMap();

                expect(Object.keys(map)).to.have.length(workerCount);
                for(var workerId in map) {
                    var worker = map[workerId];

                    expect(worker).to.be.an('object');
                    expect(worker).to.have.property('id').and.to.be.a('string');
                    expect(worker).to.have.property('lastAccess').and.to.be.a('number');
                    expect(worker).to.have.property('isAvailable').and.to.be.a('boolean');
                    expect(worker).to.have.property('pendingRequestCount').and.to.be.a('number');
                    expect(worker).to.have.property('services').and.to.be.an('Array');
                    expect(worker).to.have.property('address').and.to.be.an('object');
                }
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });
    });

    describe('getSessionMap()', function() {
        it('should return an empty object when no clients have connected', function() {
            _queue = _queueUtil.createPPQueue();

            expect(_queue.getSessionMap()).to.deep.equal({});
        });

        it('should return a copy of the map, and not a reference', function() {
            _queue = _queueUtil.createPPQueue();

            var map = _queue.getSessionMap();
            expect(map).to.deep.equal({});
            expect(_queue.getSessionMap()).not.to.equal(map);
        });

        it('should return an empty object if session affinity is not enabled, even if clients have connected', function(done) {
            var clientCount = 10;
            var clientMessage = 'MESSAGE';
            var feEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint);

            var doTests = function() {
                var map = _queue.getSessionMap();
                expect(_queue.getSessionMap()).to.deep.equal({});
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should return a map with an entry for every client that connects, if session has been enabled', function(done) {
            var clientCount = 10;
            var clientMessage = _queueUtil.createClientRequest();
            var feEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, null, {
                pollFrequency: 3000,
                workerTimeout: 9000,
                session: {
                    timeout: 30000
                }
            });

            var doTests = function() {
                var map = _queue.getSessionMap();

                expect(Object.keys(map)).to.have.length(clientCount);
                for(var clientId in map) {
                    var client = map[clientId];

                    expect(client).to.be.an('object');
                    expect(client).to.have.property('id').and.to.be.a('string');
                    expect(client).to.have.property('lastAccess').and.to.be.a('number');
                    expect(client).to.have.property('workerId').and.to.be.an('string');
                }
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });
    });

    describe('dispose()', function() {
        it('should return a promise when invoked', function(done) {
            _queue = _queueUtil.createPPQueue();

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                var ret = _queue.dispose();

                expect(ret).to.be.an('object');
                expect(ret).to.have.property('then').and.to.be.a('function');
            }).then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should stop the monitor when dispose is invoked', function(done) {
            _queue = _queueUtil.createPPQueue();

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                return _queue.dispose();
            }).then(function() {
                // Accessing a "private" variable here. Not ideal, but there
                // are no other options.
                expect(_queue._monitor.isInProgress()).to.be.false;
            }).then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should resolve the promise once the sockets have been successfully closed', function(done){
            _queue = _queueUtil.createPPQueue();

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                return expect(_queue.dispose()).to.be.fulfilled;
            }).then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should set isReady()=false when dispose succeeds', function(done) {
            _queue = _queueUtil.createPPQueue();

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                expect(_queue.isReady()).to.be.true;
                return expect(_queue.dispose()).to.be.fulfilled.then(function() {
                    expect(_queue.isReady()).to.be.false;
                });
            }).then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });
    });

    describe('[BROKER LOGIC]', function() {

        it('should increment the pending request count if a request is received and no workers are available', function(done){
            var clientCount = 3;
            var clientMessage = _queueUtil.createClientRequest();
            var feEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint);

            var doTests = function() {
                expect(_queue.getPendingRequestCount()).to.equal(clientCount);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should increment the available workers count if a worker sends a message and no requests are available', function(done){
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

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should show workers as being available if no requests have been assigned to them', function(done){
            var workerCount = 3;
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(null, beEndpoint);

            var doTests = function() {
                var map = _queue.getWorkerMap();

                for(var workerId in map) {
                    var worker = map[workerId];
                    expect(worker.isAvailable).to.be.true;
                }
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should show workers as being unavailable once a request has been assigned to them', function(done){
            var workerCount = 3;
            var clientCount = 3;
            var clientMessage = _queueUtil.createClientRequest();
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var doTests = function() {
                var map = _queue.getWorkerMap();

                for(var workerId in map) {
                    var worker = map[workerId];
                    expect(worker.isAvailable).to.be.false;
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

        it('should show workers as being unavailable once a request has been assigned to them (worker connects second)', function(done){
            var workerCount = 3;
            var clientCount = 3;
            var clientMessage = _queueUtil.createClientRequest();
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var doTests = function() {
                var map = _queue.getWorkerMap();

                for(var workerId in map) {
                    var worker = map[workerId];
                    expect(worker.isAvailable).to.be.false;
                }
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should show workers as being available once they respond to a request, with no pending requests', function(done){
            var workerCount = 3;
            var clientCount = 3;
            var clientMessage = _queueUtil.createClientRequest();
            var workerResponse = 'OK';
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            var firstCheckComplete = _q.defer();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var workerMessageHandler = function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                var socket = this;
                // Reply only after the first check is completed.
                firstCheckComplete.promise.then(function() {
                    socket.send([_messageDefinitions.RESPONSE, frames[2], frames[3], workerResponse]);
                });
            };

            var checkWorkerState = function(workerState, def) {
                return function() {
                    var map = _queue.getWorkerMap();

                    for(var workerId in map) {
                        var worker = map[workerId];
                        expect(worker.isAvailable).to.equal(workerState);
                    }
                    if(def) {
                        def.resolve();
                    }
                };
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMessageHandler))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                .then(checkWorkerState(false, firstCheckComplete))
                .then(_queueUtil.wait())

                .then(checkWorkerState(true))
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should discard messages from workers that have not yet registered', function(done) {
            var workerCount = 3;
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(null, beEndpoint);

            var doTests = function() {
                expect(_queue.getAvailableWorkerCount()).to.equal(0);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.sendMessages('foo'))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should discard all invalid requests', function(done){
            var workerCount = 3;
            var clientCount = 3;
            var clientMessage = 'INVALID';
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var doTests = function() {
                var map = _queue.getWorkerMap();

                expect(_queue.getPendingRequestCount()).to.equal(0);
                for(var workerId in map) {
                    var worker = map[workerId];
                    expect(worker.isAvailable).to.be.true;
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

        it('should raise the "REQUEST" event when a request is received from the client', function(done){
            var service = 'foo';
            var clientMessage = _queueUtil.createClientRequest(service);
            var feEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint);

            var def = _q.defer();
            _queue.on(_eventDefinitions.REQUEST, function(clientId, frames) {
                var clients = _queueUtil.getContext('client');
                _testUtil.runDeferred(function() {
                    expect(clientId.toString()).to.equal(clients[0].identity);
                    expect(frames).to.have.length(5);
                    expect(frames[0].toString()).to.equal(_messageDefinitions.REQUEST);
                    expect(frames[1].toString()).to.equal(service);
                    expect(frames[2].toString()).to.equal(clientId.toString());
                    expect(frames[3].toString()).to.be.empty;
                    expect(frames[4].toString()).to.equal(clientMessage[2]);
                }, def);
            });

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('req', 1, feEndpoint, true))
                .then(_queueUtil.captureContext('client'))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.waitForResolution(def))
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should raise the "ASSIGNED_REQUEST" event when a task has been assigned to a worker', function(done){
            var service = 'foo';
            var clientMessage = _queueUtil.createClientRequest(service);
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var def = _q.defer();
            _queue.on(_eventDefinitions.ASSIGNED_REQUEST, function(workerId, frames) {
                var clients = _queueUtil.getContext('client');
                var workers = _queueUtil.getContext('worker');
                _testUtil.runDeferred(function() {
                    expect(workerId.toString()).to.equal(workers[0].identity);

                    expect(frames).to.have.length(6);
                    expect(frames[0].toString()).to.equal(workerId.toString());
                    expect(frames[1].toString()).to.equal(_messageDefinitions.REQUEST);
                    expect(frames[2].toString()).to.equal(service);
                    expect(frames[3].toString()).to.equal(clients[0].identity);
                    expect(frames[4].toString()).to.be.empty;
                    expect(frames[5].toString()).to.equal(clientMessage[2]);
                }, def);
            });

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', 1, beEndpoint, true))
                .then(_queueUtil.captureContext('worker'))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))

                .then(_queueUtil.initSockets('req', 1, feEndpoint, true))
                .then(_queueUtil.captureContext('client'))
                .then(_queueUtil.sendMessages(clientMessage))

                .then(_queueUtil.waitForResolution(def))
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should raise the "ASSIGNED_RESPONSE" event when a response has been assigned to a client', function(done){
            var service = 'foo';
            var clientMessage = _queueUtil.createClientRequest(service);
            var workerResponse = 'OK';
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var def = _q.defer();
            _queue.on(_eventDefinitions.ASSIGNED_RESPONSE, function(clientId, frames) {
                var clients = _queueUtil.getContext('client');
                _testUtil.runDeferred(function() {
                    expect(clientId.toString()).to.equal(clients[0].identity);
                    expect(frames).to.have.length(4);
                    expect(frames[0].toString()).to.equal(clients[0].identity);
                    expect(frames[1].toString()).to.be.empty;
                    expect(frames[2].toString()).to.equal(_messageDefinitions.RESPONSE);
                    expect(frames[3].toString()).to.equal(workerResponse);
                }, def);
            });

            var workerMessageHandler = function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                this.send([_messageDefinitions.RESPONSE, frames[2], frames[3], workerResponse]);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', 1, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMessageHandler))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))

                .then(_queueUtil.initSockets('req', 1, feEndpoint, true))
                .then(_queueUtil.captureContext('client'))
                .then(_queueUtil.sendMessages(clientMessage))

                .then(_queueUtil.waitForResolution(def))
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should immediately dispatch a new request to a worker if one is available', function(done){
            var clientCount = 3;
            var workerCount = 3;
            var clientMessage = _queueUtil.createClientRequest();
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

                expect(_queue.getAvailableWorkerCount()).to.equal(0);
            var doTests = function() {
                expect(_queue.getPendingRequestCount()).to.equal(0);
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

        it('should immediately provide a newly ready worker with a request if one is available', function(done){
            var clientCount = 3;
            var workerCount = 3;
            var clientMessage = _queueUtil.createClientRequest();
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var doTests = function() {
                expect(_queue.getAvailableWorkerCount()).to.equal(0);
                expect(_queue.getPendingRequestCount()).to.equal(0);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())

                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                .then(_queueUtil.wait())

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should send the request to the worker in the correct format', function(done){
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            var service = 'foo';
            var clientMessage = _queueUtil.createClientRequest(service);
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var def = _q.defer();
            var workerMessageHandler = function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                _testUtil.runDeferred(function() {
                    expect(frames).to.have.length(5);
                    expect(frames[0].toString()).to.equal(_messageDefinitions.REQUEST);
                    expect(frames[1].toString()).to.equal(service);
                    // Frame #2 will be the client id.
                    expect(frames[3].toString()).to.be.empty;
                    expect(frames[4].toString()).to.equal(clientMessage[2]);
                }, def);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', 1, beEndpoint, true))
                .then(_queueUtil.setupHandlers('message', workerMessageHandler))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))

                .then(_queueUtil.initSockets('req', 1, feEndpoint, true))
                .then(_queueUtil.sendMessages(clientMessage))

                .then(_queueUtil.waitForResolution(def))
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should send replies from the worker back to the correct client', function(done) {
            var clientCount = 2;
            var workerCount = 2;
            var service = '';
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var workerMessageHandler = function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                this.send([_messageDefinitions.RESPONSE, frames[2], frames[3], 'OK', 'ECHO::' + frames[4].toString()]);
            };

            var sendMessagesFromClients = function() {
                var messages = Array.prototype.splice.call(arguments, 0);
                return function(sockets) {
                    var promises = [];
                    var messageIndex = 0;
                    sockets.forEach(function(socket) {
                        var def = _q.defer();
                        var message = _queueUtil.createClientRequest(service, messages[messageIndex]);
                        messageIndex = (messageIndex + 1) % messages.length;

                        socket.on('message', function() {
                            var frames = Array.prototype.splice.call(arguments, 0);
                            def.resolve([frames, message]);
                        });
                        promises.push(def.promise);

                        socket.send(message);
                    });

                    return _q.all(promises);
                };
            }

            var doTests = function(results) {
                results.forEach(function(result) {
                    var frames = result[0];
                    var message = result[1];
                    expect(frames).to.have.length(3);
                    expect(frames[0].toString()).to.equal(_messageDefinitions.RESPONSE);
                    expect(frames[1].toString()).to.equal('OK');
                    expect(frames[2].toString()).to.equal('ECHO::' + message[2]);
                });
            }

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                .then(_queueUtil.setupHandlers('message', workerMessageHandler))
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))

                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(sendMessagesFromClients('message #1', 'message #2'))

                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should discard requests that have not been processed within the configured request timeout', function(done) {
            var clientCount = 3;
            var clientMessage = _queueUtil.createClientRequest();
            var requestTimeout = 300;
            var pollFrequency = 100;
            var feEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, null, {
                pollFrequency: 100,
                workerTimeout: 2000,
                requestTimeout: requestTimeout
            });

            var checkRequestCount = function(count) {
                return function(sockets) {
                    expect(_queue.getPendingRequestCount()).to.equal(count);
                    return sockets;
                };
            }

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                .then(_queueUtil.sendMessages(clientMessage))
                .then(_queueUtil.wait())
                
                .then(checkRequestCount(clientCount))
                .then(_queueUtil.wait(pollFrequency * 4))

                .then(checkRequestCount(0))
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

    });
});
