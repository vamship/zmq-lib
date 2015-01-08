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

var ParanoidPirateQueue = require('../../lib/paranoid-pirate-queue');
var _messageDefinitions = require('../../lib/message-definitions');
var _eventDefinitions = require('../../lib/event-definitions');


describe('ParanoidPirateQueue', function() {
    var DEFAULT_DELAY = 10;
    var _queue;
    var _sockets;

    beforeEach(function(){
        _sockets = [];
    })

    afterEach(function(){
        if(_queue) {
            _queue.dispose();
        }
        _sockets.forEach(function(sock){
            sock.close();
        });
    });

    function _getSuccessCallback(done) {
        return function(){
            done();
        }
    }

    function _getFailureCallback(done) {
        return function(err) {
            done(err);
        }
    }

    function _createWorkerOptions(pollFrequency, workerTimeout) {
        pollFrequency = pollFrequency || 1000;
        workerTimeout = workerTimeout || 3000;

        return {
            pollFrequency: pollFrequency,
            workerTimeout: workerTimeout
        };
    }

    function _createQueue(feEndpoint, beEndpoint, workerOptions) {
        feEndpoint = feEndpoint || _testUtils.generateEndpoint();
        beEndpoint = beEndpoint || _testUtils.generateEndpoint();
        workerOptions = workerOptions || _createWorkerOptions();
        return new ParanoidPirateQueue(feEndpoint, beEndpoint, workerOptions);
    }

    function _createReqSocket(id) {
        var socket =  _zmq.createSocket('req');
        if(id) {
            socket.identity = id;
        }
        socket.monitor(10);
        _sockets.push(socket);
        
        return socket;
    }

    function _createDealerSocket(id) {
        var socket =  _zmq.createSocket('dealer');
        if(id) {
            socket.identity = id;
        }
        socket.monitor(10);
        _sockets.push(socket);
        
        return socket;
    }

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
            var feEndpoint = _testUtils.generateEndpoint();
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

        it('should throw an error if valid worker options are not specified', function() {
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            var error = 'invalid worker options specified (arg #3)';

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

        it('should throw an error if the worker options does not include a poll frequency', function() {
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            var error = 'worker options does not define a poll frequency property (workerOptions.pollFrequency)';

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

        it('should throw an error if the worker options does not include a worker timeout', function() {
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            var error = 'worker options does not define a worker timeout property (workerOptions.workerTimeout)';

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

        it('should create an object that exposes members required by the interface', function() {
            _queue = _createQueue();

            expect(_queue).to.be.an.instanceof(_events.EventEmitter);
            expect(_queue).to.have.property('initialize').and.to.be.a('function');
            expect(_queue).to.have.property('dispose').and.to.be.a('function');
            expect(_queue).to.have.property('getPendingRequestCount').and.to.be.a('function');
            expect(_queue).to.have.property('getAvailableWorkerCount').and.to.be.a('function');
            expect(_queue).to.have.property('getWorkerMap').and.to.be.a('function');
            expect(_queue).to.have.property('isReady').and.to.be.a('function');
        });

        it('should initialize properties to default values', function() {
            _queue = _createQueue();

            expect(_queue.getPendingRequestCount()).to.equal(0);
            expect(_queue.getAvailableWorkerCount()).to.equal(0);
            expect(_queue.getWorkerMap()).to.deep.equal({});
            expect(_queue.isReady()).to.be.false;
        });
    });

    describe('initialize()', function() {
        it('should return a promise when invoked', function(){
            _queue = _createQueue();

            var ret = _queue.initialize();
            expect(ret).to.be.an('object');
            expect(ret).to.have.property('then').and.to.be.a('function');
        });

        it('should reject the promise if the front end binding fails', function(done) {
            _queue = _createQueue('bad-endpoint');
            
            expect(_queue.initialize()).to.be.rejected.notify(done);
        });

        it('should reject the promise if the back end binding fails', function(done){
            _queue = _createQueue(null, 'bad-endpoint');

            expect(_queue.initialize()).to.be.rejected.notify(done);
        });

        it('should reject the promise if both front and back end binrings fail', function(done){
            _queue = _createQueue('bad-endpoint', 'bad-endpoint');

            expect(_queue.initialize()).to.be.rejected.notify(done);
        });

        it('should resolve the promise once both front and back ends have been bound successfully', function(done){
            _queue = _createQueue();

            expect(_queue.initialize()).to.be.fulfilled.notify(done);
        });

        it('should set isReady()=true when initialization succeeds', function(done) {
            _queue = _createQueue();

            expect(_queue.isReady()).to.be.false;

            expect(_queue.initialize()).to.be.fulfilled.then(function() {
                expect(_queue.isReady()).to.be.true;
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should set isReady()=false when initialization fails', function(done) {
            _queue = _createQueue('bad-endpoint', 'bad-endpoint');

            expect(_queue.isReady()).to.be.false;

            expect(_queue.initialize()).to.be.rejected.then(function() {
                expect(_queue.isReady()).to.be.false;
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should open a front end socket and bind to the endpoint when invoked', function(done) {
            var feEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(feEndpoint);

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                var def = _q.defer();
                var client1 = _createReqSocket();

                client1.on('connect', function(){
                    def.resolve();
                });
                client1.connect(feEndpoint);

                return def.promise;
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should open a back end socket and bind to the endpoint when invoked', function(done) {
            var beEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(beEndpoint);

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                var def = _q.defer();
                var worker = _createDealerSocket();

                worker.on('connect', function(){
                    def.resolve();
                });
                worker.connect(beEndpoint);

                return def.promise;
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });
    });

    describe('dispose()', function() {
        it('should return a promise when invoked', function(done) {
            _queue = _createQueue();

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                var ret = _queue.dispose();

                expect(ret).to.be.an('object');
                expect(ret).to.have.property('then').and.to.be.a('function');
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should stop the monitor when dispose is invoked', function(done) {
            _queue = _createQueue();

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                return _queue.dispose();
            }).then(function() {
                // Accessing a "private" variable here. Not ideal, but there
                // are no other options.
                expect(_queue._monitor.isInProgress()).to.be.false;
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should resolve the promise once the sockets have been successfully closed', function(done){
            _queue = _createQueue();

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                return expect(_queue.dispose()).to.be.fulfilled;
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should set isReady()=false when dispose succeeds', function(done) {
            _queue = _createQueue();

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                expect(_queue.isReady()).to.be.true;
                return expect(_queue.dispose()).to.be.fulfilled.then(function() {
                    expect(_queue.isReady()).to.be.false;
                });
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });
    });

    describe('[BROKER LOGIC]', function() {

        function _getResolver(def, socket) {
            return function() {
               def.resolve(socket); 
            };
        }

        function _wait(delay) {
            delay = delay || DEFAULT_DELAY;
            return _testUtils.getDelayedRunner(function(data) {
                return data;
            }, delay);
        }

        function _waitForResolution(def) {
            return function() {
                return def.promise;
            };
        }

        function _createAndConnectSockets(type, count, endpoint, setIds) {
            return function() {
                var promises = [];

                for(var index=1; index<=count; index++) {
                    var def = _q.defer();
                    var id = setIds? _uuid.v4():null;
                    var socket = (type === 'client')? _createReqSocket(id):
                                                      _createDealerSocket(id);

                    socket.connect(endpoint);
                    socket.on('connect', _getResolver(def, socket));

                    promises.push(def.promise);
                }
                return _q.all(promises);
            };
        }

        function _sendMessagesOverSockets() {
            var messages = Array.prototype.splice.call(arguments, 0);
            return function(sockets) {
                var messageIndex = 0;
                sockets.forEach(function(socket) {
                    var message = messages[messageIndex];
                    messageIndex = (messageIndex + 1) % messages.length;

                    socket.send(message);
                });

                return sockets;
            };
        }

        function _setupSocketHandlers(event, handler) {
            return function(sockets) {
                sockets.forEach(function(socket) {
                    socket.on(event, handler.bind(socket));
                });

                return sockets;
            };
        }

        it('should increment the pending request count if a request is received and no workers are available', function(done){
            var clientCount = 3;
            var clientMessage = 'MESSAGE';
            var feEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(feEndpoint);

            var doTests = function() {
                expect(_queue.getPendingRequestCount()).to.equal(clientCount);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_createAndConnectSockets('client', clientCount, feEndpoint))
                .then(_sendMessagesOverSockets(clientMessage))
                .then(_wait())

                .then(doTests)
                .then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should increment the available workers count if a worker sends a message and no requests are available', function(done){
            var workerCount = 3;
            var beEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(null, beEndpoint);

            var doTests = function() {
                expect(_queue.getAvailableWorkerCount()).to.equal(workerCount);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_createAndConnectSockets('worker', workerCount, beEndpoint))
                .then(_sendMessagesOverSockets(_messageDefinitions.READY))
                .then(_wait())

                .then(doTests)
                .then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should raise the "REQUEST" event when a request is received from the client', function(done){
            var client = null;
            var clientMessage = 'MESSAGE';
            var feEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(feEndpoint);

            var captureClientRef = function(clients) {
                client = clients[0];
                return clients;
            };

            var def = _q.defer();
            _queue.on(_eventDefinitions.REQUEST, function(clientId, frames) {
                _testUtils.runDeferred(function() {
                    expect(clientId.toString()).to.equal(client.identity);
                    expect(frames).to.have.length(3);
                    expect(frames[0].toString()).to.equal(clientId.toString());
                    expect(frames[1].toString()).to.be.empty;
                    expect(frames[2].toString()).to.equal(clientMessage);
                }, def);
            });

            expect(_queue.initialize()).to.be.fulfilled
                .then(_createAndConnectSockets('client', 1, feEndpoint, true))
                .then(captureClientRef)
                .then(_sendMessagesOverSockets(clientMessage))
                .then(_waitForResolution(def))
                .then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should raise the "ASSIGNED_REQUEST" event when a task has been assigned to a worker', function(done){
            var worker = null;
            var client = null;
            var clientMessage = 'MESSAGE';
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(feEndpoint, beEndpoint);

            var captureClientRef = function(clients) {
                client = clients[0];
                return clients;
            };

            var captureWorkerRef = function(workers) {
                worker = workers[0];
                return workers;
            };

            var def = _q.defer();
            _queue.on(_eventDefinitions.ASSIGNED_REQUEST, function(workerId, frames) {
                _testUtils.runDeferred(function() {
                    expect(workerId.toString()).to.equal(worker.identity);

                    expect(frames).to.have.length(5);
                    expect(frames[0].toString()).to.equal(workerId.toString());
                    expect(frames[1].toString()).to.equal(_messageDefinitions.REQUEST);
                    expect(frames[2].toString()).to.equal(client.identity);
                    expect(frames[3].toString()).to.be.empty;
                    expect(frames[4].toString()).to.equal(clientMessage);
                }, def);
            });

            expect(_queue.initialize()).to.be.fulfilled
                .then(_createAndConnectSockets('worker', 1, beEndpoint, true))
                .then(captureWorkerRef)
                .then(_sendMessagesOverSockets(_messageDefinitions.READY))

                .then(_createAndConnectSockets('client', 1, feEndpoint, true))
                .then(captureClientRef)
                .then(_sendMessagesOverSockets(clientMessage))

                .then(_waitForResolution(def))
                .then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should raise the "ASSIGNED_RESPONSE" event when a response has been assigned to a client', function(done){
            var clientMessage = 'MESSAGE';
            var workerResponse = 'OK';
            var client = null;
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(feEndpoint, beEndpoint);

            var captureClientRef = function(clients) {
                client = clients[0];
                return clients;
            };

            var def = _q.defer();
            _queue.on(_eventDefinitions.ASSIGNED_RESPONSE, function(clientId, frames) {
                _testUtils.runDeferred(function() {
                    expect(clientId.toString()).to.equal(client.identity);
                    expect(frames).to.have.length(3);
                    expect(frames[0].toString()).to.equal(client.identity);
                    expect(frames[1].toString()).to.be.empty;
                    expect(frames[2].toString()).to.equal(workerResponse);
                }, def);
            });

            var workerMessageHandler = function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                this.send([frames[1], frames[2], workerResponse]);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_createAndConnectSockets('worker', 1, beEndpoint, true))
                .then(_setupSocketHandlers('message', workerMessageHandler))
                .then(_sendMessagesOverSockets(_messageDefinitions.READY))

                .then(_createAndConnectSockets('client', 1, feEndpoint, true))
                .then(captureClientRef)
                .then(_sendMessagesOverSockets(clientMessage))

                .then(_waitForResolution(def))
                .then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should immediately dispatch a new request to a worker if one is available', function(done){
            var clientCount = 3;
            var workerCount = 3;
            var clientMessage = 'MESSAGE';
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(feEndpoint, beEndpoint);

            var doTests = function() {
                expect(_queue.getAvailableWorkerCount()).to.equal(0);
                expect(_queue.getPendingRequestCount()).to.equal(0);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_createAndConnectSockets('worker', workerCount, beEndpoint))
                .then(_sendMessagesOverSockets(_messageDefinitions.READY))
                .then(_wait())

                .then(_createAndConnectSockets('client', clientCount, feEndpoint))
                .then(_sendMessagesOverSockets(clientMessage))
                .then(_wait())

                .then(doTests)
                .then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should immediately provide a newly ready worker with a request if one is available', function(done){
            var clientCount = 3;
            var workerCount = 3;
            var clientMessage = 'MESSAGE';
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(feEndpoint, beEndpoint);

            var doTests = function() {
                expect(_queue.getAvailableWorkerCount()).to.equal(0);
                expect(_queue.getPendingRequestCount()).to.equal(0);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_createAndConnectSockets('client', clientCount, feEndpoint))
                .then(_sendMessagesOverSockets(clientMessage))
                .then(_wait())

                .then(_createAndConnectSockets('worker', workerCount, beEndpoint))
                .then(_sendMessagesOverSockets(_messageDefinitions.READY))
                .then(_wait())

                .then(doTests)
                .then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should send the request to the worker in the correct format', function(done){
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            var clientMessage = 'MESSAGE';
            _queue = _createQueue(feEndpoint, beEndpoint);

            var def = _q.defer();
            var workerMessageHandler = function() {
                var frames = Array.prototype.splice.call(arguments, 0);
                _testUtils.runDeferred(function() {
                    expect(frames).to.have.length(4);
                    expect(frames[0].toString()).to.equal(_messageDefinitions.REQUEST);
                    expect(frames[2].toString()).to.equal('');
                    expect(frames[3].toString()).to.equal(clientMessage);
                }, def);
            };

            expect(_queue.initialize()).to.be.fulfilled
                .then(_createAndConnectSockets('worker', 1, beEndpoint, true))
                .then(_setupSocketHandlers('message', workerMessageHandler))
                .then(_sendMessagesOverSockets(_messageDefinitions.READY))

                .then(_createAndConnectSockets('client', 1, feEndpoint, true))
                .then(_sendMessagesOverSockets(clientMessage))

                .then(_waitForResolution(def))
                .then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should send replies from the worker back to the correct client', function(done) {
            var clientCount = 2;
            var workerCount = 2;
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(feEndpoint, beEndpoint);

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
                .then(_createAndConnectSockets('worker', workerCount, beEndpoint))
                .then(_setupSocketHandlers('message', workerMessageHandler))
                .then(_sendMessagesOverSockets(_messageDefinitions.READY))

                .then(_createAndConnectSockets('client', clientCount, feEndpoint))
                .then(sendMessagesFromClients('message #1', 'message #2'))

                .then(doTests)
                .then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        describe('[HEARTBEAT/EXPIRY LOGIC]', function() {

            it('should store worker metadata when a new worker makes a connection', function(done) {
                var workerCount = 3;
                var beEndpoint = _testUtils.generateEndpoint();
                _queue = _createQueue(null, beEndpoint);

                var doTests = function() {
                    var knownWorkers = _queue.getWorkerMap();
                    var now = Date.now();

                    expect(knownWorkers).to.not.be.empty;
                    for(var workerId in knownWorkers) {
                        var worker = knownWorkers[workerId];
                        
                        expect(worker).to.be.an('object');
                        expect(worker).to.have.property('address').and.to.be.an('object');
                        expect(worker).to.have.property('id', workerId);
                        expect(worker).to.have.property('lastContact').and.to.be.within(now-50, now+50);
                    }
                };

                expect(_queue.initialize()).to.be.fulfilled
                    .then(_createAndConnectSockets('worker', workerCount, beEndpoint))
                    .then(_sendMessagesOverSockets(_messageDefinitions.READY))
                    .then(_wait())

                    .then(doTests)
                    .then(_getSuccessCallback(done), _getFailureCallback(done));
            });

            it('should update worker timestamp when the worker sends a heartbeat', function(done) {
                var workerCount = 3;
                var feEndpoint = _testUtils.generateEndpoint();
                var beEndpoint = _testUtils.generateEndpoint();
                var initialWorkerMap = null;
                _queue = _createQueue(feEndpoint, beEndpoint);

                var captureInitialWorkerMap = function(workers) {
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
                        expect(initialWorkerInfo.lastContact).to.be.below(workerInfo.lastContact);
                    }
                };

                expect(_queue.initialize()).to.be.fulfilled
                    .then(_createAndConnectSockets('worker', workerCount, beEndpoint))
                    .then(_sendMessagesOverSockets(_messageDefinitions.READY))
                    .then(_wait())

                    .then(captureInitialWorkerMap)
                    .then(_sendMessagesOverSockets(_messageDefinitions.HEARTBEAT))
                    .then(_wait())

                    .then(doTests)
                    .then(_getSuccessCallback(done), _getFailureCallback(done));
            });

            it('should update worker timestamp when the worker replies to a request', function(done) {
                var workerCount = 3;
                var clientCount = 3;
                var workerResponse = 'OK';
                var feEndpoint = _testUtils.generateEndpoint();
                var beEndpoint = _testUtils.generateEndpoint();
                var initialWorkerMap = null;
                _queue = _createQueue(feEndpoint, beEndpoint);

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

                var captureInitialWorkerMap = function(workers) {
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
                        expect(initialWorkerInfo.lastContact).to.be.below(workerInfo.lastContact);
                    }
                };

                expect(_queue.initialize()).to.be.fulfilled
                    .then(_createAndConnectSockets('worker', workerCount, beEndpoint))
                    .then(_setupSocketHandlers('message', workerMessageHandler))
                    .then(_sendMessagesOverSockets(_messageDefinitions.READY))
                    .then(_wait())

                    .then(captureInitialWorkerMap)
                    .then(_createAndConnectSockets('client', clientCount, feEndpoint))
                    .then(_sendMessagesOverSockets('message #1', 'message #2'))
                    .then(_waitForResolution(def))

                    .then(doTests)
                    .then(_getSuccessCallback(done), _getFailureCallback(done));
            });

            it('should respond to a heartbeat with another heartbeat', function(done) {
                var workerCount = 2;
                var feEndpoint = _testUtils.generateEndpoint();
                var beEndpoint = _testUtils.generateEndpoint();
                _queue = _createQueue(feEndpoint, beEndpoint);

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
                    .then(_createAndConnectSockets('worker', workerCount, beEndpoint))
                    .then(_setupSocketHandlers('message', workerMessageHandler))
                    .then(_sendMessagesOverSockets(_messageDefinitions.READY))
                    .then(_wait())

                    .then(_sendMessagesOverSockets(_messageDefinitions.HEARTBEAT))
                    .then(_waitForResolution(def))

                    .then(doTests)
                    .then(_getSuccessCallback(done), _getFailureCallback(done));
            });

            it('should not add a worker sending a heartbeat to the available workers list', function(done) {
                var workerCount = 3;
                var beEndpoint = _testUtils.generateEndpoint();
                _queue = _createQueue(null, beEndpoint);

                var doTests = function() {
                    expect(_queue.getAvailableWorkerCount()).to.equal(workerCount);
                };

                expect(_queue.initialize()).to.be.fulfilled
                    .then(_createAndConnectSockets('worker', workerCount, beEndpoint))
                    .then(_sendMessagesOverSockets(_messageDefinitions.READY))
                    .then(_wait())

                    .then(_sendMessagesOverSockets(_messageDefinitions.HEARTBEAT))
                    .then(_wait())

                    .then(doTests)
                    .then(_getSuccessCallback(done), _getFailureCallback(done));
            });

            it('should not send a pending request to a worker sending a heartbeat', function(done) {
                var workerCount = 3;
                var clientCount = 4;
                var clientMessage = 'MESSAGE';
                var feEndpoint = _testUtils.generateEndpoint();
                var beEndpoint = _testUtils.generateEndpoint();
                _queue = _createQueue(feEndpoint, beEndpoint);

                var doTests = function() {
                    expect(_queue.getPendingRequestCount()).to.equal(clientCount - workerCount);
                };

                expect(_queue.initialize()).to.be.fulfilled
                    .then(_createAndConnectSockets('client', clientCount, feEndpoint))
                    .then(_sendMessagesOverSockets(clientMessage))
                    .then(_wait())

                    .then(_createAndConnectSockets('worker', workerCount, beEndpoint))
                    .then(_sendMessagesOverSockets(_messageDefinitions.READY))
                    .then(_wait())

                    .then(_sendMessagesOverSockets(_messageDefinitions.HEARTBEAT))
                    .then(_wait())

                    .then(doTests)
                    .then(_getSuccessCallback(done), _getFailureCallback(done));
            });

            it('should discard a worker if workerTimeout expires since last contact with the worker', function(done) {
                var workerCount = 3;
                var pollFrequency = 100;
                var workerTimeout = 300;
                var beEndpoint = _testUtils.generateEndpoint();

                _queue = _createQueue(null, beEndpoint, {
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
                    .then(_createAndConnectSockets('worker', workerCount, beEndpoint))
                    .then(_sendMessagesOverSockets(_messageDefinitions.READY))
                    .then(_wait())

                    .then(verifyWorkerCount)
                    .then(_wait(pollFrequency * 4))

                    .then(doTests)
                    .then(_getSuccessCallback(done), _getFailureCallback(done));
            });

            it('should not send a request to a worker whose timeout has expired', function(done) {
                var clientCount = 3;
                var workerCount = 3;
                var clientMessage = 'MESSAGE';
                var pollFrequency = 100;
                var workerTimeout = 300;
                var expiredWorkerHandler = _sinon.spy();
                var validWorkerHandler = _sinon.spy();
                var beEndpoint = _testUtils.generateEndpoint();
                var feEndpoint = _testUtils.generateEndpoint();

                _queue = _createQueue(feEndpoint, beEndpoint, {
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
                    .then(_createAndConnectSockets('worker', workerCount, beEndpoint, true))
                    .then(_setupSocketHandlers('message', expiredWorkerHandler))
                    .then(_sendMessagesOverSockets(_messageDefinitions.READY))
                    .then(_wait(pollFrequency * 4))

                    .then(verifyWorkerCount)

                    .then(_createAndConnectSockets('worker', workerCount, beEndpoint))
                    .then(_setupSocketHandlers('message', validWorkerHandler))
                    .then(_sendMessagesOverSockets(_messageDefinitions.READY))
                    .then(_wait())

                    .then(_createAndConnectSockets('client', workerCount, feEndpoint))
                    .then(_sendMessagesOverSockets(clientMessage))
                    .then(_wait())

                    .then(doTests)
                    .then(_getSuccessCallback(done), _getFailureCallback(done));
            });
        });
    });
});
