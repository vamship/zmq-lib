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
                .then(_queueUtil.sendMessages(_messageDefinitions.READY))
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
            var clientMessage = 'MESSAGE';
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
            var clientMessage = 'MESSAGE';
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
            var clientMessage = 'MESSAGE';
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
            var clientMessage = 'MESSAGE';
            var workerResponse = 'OK';
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            var firstCheckComplete = _q.defer();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var workerMessageHandler = function() {
                var socket = this;
                // Reply only after the first check is completed.
                firstCheckComplete.promise.then(function() {
                    var frames = Array.prototype.splice.call(arguments, 0);
                    socket.send([frames[1], frames[2], workerResponse]);
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

        it('should raise the "REQUEST" event when a request is received from the client', function(done){
            var clientMessage = 'MESSAGE';
            var feEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint);

            var def = _q.defer();
            _queue.on(_eventDefinitions.REQUEST, function(clientId, frames) {
                var clients = _queueUtil.getContext('client');
                _testUtil.runDeferred(function() {
                    expect(clientId.toString()).to.equal(clients[0].identity);
                    expect(frames).to.have.length(3);
                    expect(frames[0].toString()).to.equal(clientId.toString());
                    expect(frames[1].toString()).to.be.empty;
                    expect(frames[2].toString()).to.equal(clientMessage);
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
            var clientMessage = 'MESSAGE';
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var def = _q.defer();
            _queue.on(_eventDefinitions.ASSIGNED_REQUEST, function(workerId, frames) {
                var clients = _queueUtil.getContext('client');
                var workers = _queueUtil.getContext('worker');
                _testUtil.runDeferred(function() {
                    expect(workerId.toString()).to.equal(workers[0].identity);

                    expect(frames).to.have.length(5);
                    expect(frames[0].toString()).to.equal(workerId.toString());
                    expect(frames[1].toString()).to.equal(_messageDefinitions.REQUEST);
                    expect(frames[2].toString()).to.equal(clients[0].identity);
                    expect(frames[3].toString()).to.be.empty;
                    expect(frames[4].toString()).to.equal(clientMessage);
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
            var clientMessage = 'MESSAGE';
            var workerResponse = 'OK';
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var def = _q.defer();
            _queue.on(_eventDefinitions.ASSIGNED_RESPONSE, function(clientId, frames) {
                var clients = _queueUtil.getContext('client');
                _testUtil.runDeferred(function() {
                    expect(clientId.toString()).to.equal(clients[0].identity);
                    expect(frames).to.have.length(3);
                    expect(frames[0].toString()).to.equal(clients[0].identity);
                    expect(frames[1].toString()).to.be.empty;
                    expect(frames[2].toString()).to.equal(workerResponse);
                }, def);
            });

            var workerMessageHandler = function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                this.send([frames[1], frames[2], workerResponse]);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_queueUtil.initSockets('dealer', 1, beEndpoint, true))
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
            var clientMessage = 'MESSAGE';
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var doTests = function() {
                expect(_queue.getAvailableWorkerCount()).to.equal(0);
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
            var clientMessage = 'MESSAGE';
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
            var clientMessage = 'MESSAGE';
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var def = _q.defer();
            var workerMessageHandler = function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                _testUtil.runDeferred(function() {
                    expect(frames).to.have.length(4);
                    expect(frames[0].toString()).to.equal(_messageDefinitions.REQUEST);
                    expect(frames[2].toString()).to.equal('');
                    expect(frames[3].toString()).to.equal(clientMessage);
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
            var feEndpoint = _testUtil.generateEndpoint();
            var beEndpoint = _testUtil.generateEndpoint();
            _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint);

            var workerMessageHandler = function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                this.send([frames[1], frames[2], 'OK', 'ECHO::' + frames[3].toString()]);
            };

            var sendMessagesFromClients = function() {
                var messages = Array.prototype.splice.call(arguments, 0);
                return function(sockets) {
                    var promises = [];
                    var messageIndex = 0;
                    sockets.forEach(function(socket) {
                        var def = _q.defer();
                        socket.on('message', function() {
                            var frames = Array.prototype.splice.call(arguments, 0);
                            def.resolve([frames, message]);
                        });
                        promises.push(def.promise);

                        var message = messages[messageIndex];
                        messageIndex = (messageIndex + 1) % messages.length;

                        socket.send(message);
                    });

                    return _q.all(promises);
                };
            }

            var doTests = function(results) {
                results.forEach(function(result) {
                    var frames = result[0];
                    var message = result[1];
                    expect(frames).to.have.length(2);
                    expect(frames[0].toString()).to.equal('OK');
                    expect(frames[1].toString()).to.equal('ECHO::' + message);
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
            var clientMessage = 'MESSAGE';
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
                    .then(_queueUtil.wait())

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
                    this.send([frames[0], frames[1], workerResponse]);
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

                expect(_queue.initialize()).to.be.fulfilled
                    .then(_queueUtil.initSockets('dealer', workerCount, beEndpoint))
                    .then(_queueUtil.setupHandlers('message', workerMessageHandler))
                    .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                    .then(_queueUtil.wait())

                    .then(saveInitialWorkerMap)
                    .then(_queueUtil.initSockets('req', clientCount, feEndpoint))
                    .then(_queueUtil.sendMessages('message #1', 'message #2'))
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
                var clientMessage = 'MESSAGE';
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
                var clientMessage = 'MESSAGE';
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
                var clientMessage = 'MESSAGE';
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

            function _initializeWorkers(workerIds) {
                var workerMap = {};
                var handlers = [];
                var workerList = [];
                var count = 0;
                workerIds.forEach(function(workerId) {
                    var worker = {
                        id: workerId,
                        messages: [],
                        handler: function() {}
                    }

                    workerMap[workerId] = worker;
                    worker.handler = _sinon.stub(worker, 'handler', function() {
                        var frames = Array.prototype.splice.call(arguments, 0);

                        worker.messages.push(frames[3].toString());
                        if(frames.length < 5 || frames[4].toString() !== 'block') {
                            this.send([frames[1], frames[2], 'OK']);
                            worker.unblock = function() {};
                        } else {
                            var socket = this;
                            worker.unblock = function() {
                                socket.send([frames[1], frames[2], 'OK']);
                            };
                        }
                    });

                    workerList.push(worker);
                    handlers.push(worker.handler);
                    count++;
                });

                workerMap._workerList = workerList;
                workerMap._handlers = handlers;
                workerMap._count = count;
                return workerMap;
            }

            function _unblockWorkers(workerMap) {
                return function(data) {
                    workerMap._workerList.forEach(function(worker) {
                        worker.unblock();
                    });
                    return data;
                };
            }

            it('should distribute requests from one client to multiple workers when session has not been enabled', function(done) {
                var feEndpoint = _testUtil.generateEndpoint();
                var beEndpoint = _testUtil.generateEndpoint();
                _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint, {
                    pollFrequency: 5000,
                    workerTimeout: 10000
                });

                var clientIds = [ 'client1', 'client2' ];
                var workerMap = _initializeWorkers(['worker1', 'worker2']);

                var doTests = function() {
                    workerMap._workerList.forEach(function(worker) {
                        expect(worker.messages).to.include.members(clientIds);
                    });
                };

                var sendMessagesInReverseOrder = function(sockets) {
                    sockets[1].send(clientIds[1]);
                    sockets[0].send(clientIds[0]);
                    return sockets;
                };

                expect(_queue.initialize()).to.be.fulfilled
                    .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint))
                    .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                    .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                    .then(_queueUtil.wait())

                    .then(_queueUtil.initSockets('req', clientIds.length, feEndpoint))
                    .then(_queueUtil.sendMessages(clientIds[0], clientIds[1]))
                    .then(_queueUtil.wait())

                    // Send messages in reverse order, so that they are distributed across workers.
                    .then(sendMessagesInReverseOrder)
                    .then(_queueUtil.wait())

                    .then(doTests)
                    .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
            });

            it('should create a new session, and add the client request to the pending request queue if no workers are available', function(done) {
                var clientCount = 3;
                var clientMessage = 'MESSAGE';
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
                var clientMessage = 'MESSAGE';
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
                var clientMessage = 'MESSAGE';
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
                var workerMap = _initializeWorkers(workerIds);

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
                        sockets[index].send(clientIds[index]);
                    }
                    return sockets;
                };

                expect(_queue.initialize()).to.be.fulfilled
                    .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint))
                    .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                    .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                    .then(_queueUtil.wait())

                    .then(_queueUtil.initSockets('req', clientIds.length, feEndpoint))
                    .then(_queueUtil.sendMessages(clientIds[0], clientIds[1],
                                                   clientIds[2], clientIds[3]))
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
                var workerMap = _initializeWorkers(workerIds);

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
                        sockets[index].send(['', clientIds[index]]);
                    }
                    return sockets;
                };

                expect(_queue.initialize()).to.be.fulfilled
                    .then(_queueUtil.initSockets('dealer', clientIds.length, feEndpoint))
                    .then(_queueUtil.captureContext('client'))
                    .then(_queueUtil.sendMessages(['', clientIds[0]], ['', clientIds[1]],
                                                   ['', clientIds[2]], ['', clientIds[3]]))
                    .then(_queueUtil.wait())

                    // Send messages in reverse order, but expect session affinity to be honored
                    .then(_queueUtil.switchContext('client'))
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
                var clientMessage = 'MESSAGE';
                _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint, {
                    pollFrequency: 5000,
                    workerTimeout: 10000,
                    session: {
                        timeout: 30000
                    }
                });

                var workerIds = [ 'worker1' ];
                var clientIds = [ 'client1', 'client2', 'client3' ];
                var workerMap = _initializeWorkers(workerIds);

                var doTests = function() {
                    var workerList = workerMap._workerList;

                    expect(workerList[0].messages).to.deep.equal([
                        clientIds[0], clientIds[1], clientIds[2],
                        clientMessage, clientMessage, clientMessage,
                        clientIds[0], clientIds[1], clientIds[2]
                    ]);
                };

                expect(_queue.initialize()).to.be.fulfilled
                    .then(_queueUtil.initSockets('req', clientIds.length, feEndpoint))
                    .then(_queueUtil.captureContext('client'))
                    .then(_queueUtil.sendMessages(clientIds[0],
                                                   clientIds[1],
                                                   [clientIds[2], 'block']))
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
                    .then(_queueUtil.sendMessages(clientIds[0],
                                                   clientIds[1],
                                                   [clientIds[2], 'block']))
                    .then(_queueUtil.wait())

                    //Unblock the workers
                    .then(_unblockWorkers(workerMap))
                    .then(_queueUtil.wait())

                    .then(doTests)
                    .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
            });

            it('should assign a request to the session defined worker, even if the worker has died', function(done) {
                var clientCount = 3;
                var clientMessage = 'MESSAGE';
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
                var firstWorkerMap = _initializeWorkers(workerIds);
                var secondWorkerMap = _initializeWorkers(workerIds);

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
                var workerMap = _initializeWorkers(workerIds);

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

                    sockets[1].send([clientIds[1], 'no-block']);
                    sockets[0].send([clientIds[0], 'no-block']);
                    return sockets;
                };

                expect(_queue.initialize()).to.be.fulfilled
                    .then(_queueUtil.initSockets('dealer', workerMap._count, beEndpoint, true))
                    .then(_queueUtil.setupHandlers('message', workerMap._handlers))
                    .then(_queueUtil.sendMessages(_messageDefinitions.READY))
                    .then(_queueUtil.wait())

                    .then(_queueUtil.initSockets('req', clientIds.length, feEndpoint))
                    .then(_queueUtil.sendMessages([clientIds[0], 'no-block'],
                                                   [clientIds[1], 'no-block'],
                                                   [clientIds[2], 'block']))
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
                var workerMap = _initializeWorkers(workerIds);

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

                    sockets[1].send([clientIds[1], 'no-block']);
                    sockets[0].send([clientIds[0], 'no-block']);
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
                    .then(_queueUtil.sendMessages([clientIds[0], 'no-block'],
                                                   [clientIds[1], 'no-block'],
                                                   [clientIds[2], 'block']))
                    .then(_queueUtil.wait())

                    .then(checkStateAndSendMessages)
                    .then(_queueUtil.wait())

                    .then(checkRequestCount)
                    .then(_unblockWorkers(workerMap))
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
                var workerMap = _initializeWorkers(workerIds);

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
                    .then(_queueUtil.sendMessages([clientIds[0], 'block'],
                                                   [clientIds[0], 'block'],
                                                   [clientIds[0], 'block']))
                    .then(_queueUtil.wait())

                    .then(checkAvailableWorkers(0))
                    .then(_queueUtil.switchContext('worker'))
                    .then(_unblockWorkers(workerMap))
                    .then(_queueUtil.wait())

                    .then(checkAvailableWorkers(workerIds.length))

                    .then(_queueUtil.switchContext('client'))
                    .then(_queueUtil.sendMessages([clientIds[0], 'block'],
                                                   [clientIds[0], 'block'],
                                                   [clientIds[0], 'block']))
                    .then(_queueUtil.wait())

                    .then(checkAvailableWorkers(0))
                    .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
            });

            it('should update session timestamp when a client sends a request', function(done) {
                var clientCount = 3;
                var feEndpoint = _testUtil.generateEndpoint();
                var beEndpoint = _testUtil.generateEndpoint();
                var clientMessage = 'MESSAGE';
                _queue = _queueUtil.createPPQueue(feEndpoint, beEndpoint, {
                    pollFrequency: 5000,
                    workerTimeout: 10000,
                    session: {
                        timeout: 30000
                    }
                });

                var workerIds = [ 'worker1' ];
                var workerMap = _initializeWorkers(workerIds);

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
                var clientMessage = 'MESSAGE';
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
});
