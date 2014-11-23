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
var SimpleQueue = require('../../lib/simple-queue');
var _messageDefinitions = require('../../lib/message-definitions');
var _eventDefinitions = require('../../lib/event-definitions');

describe('SimpleQueue', function() {
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

    function _createQueue(feEndpoint, beEndpoint) {
        feEndpoint = feEndpoint || _testUtils.generateEndpoint();
        beEndpoint = beEndpoint || _testUtils.generateEndpoint();
        return new SimpleQueue(feEndpoint, beEndpoint);
    }

    function _createReqSocket() {
        var socket =  _zmq.createSocket('req');
        socket.monitor(10);
        _sockets.push(socket);
        
        return socket;
    }

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

    describe('ctor()', function() {
        it('should throw an error if invoked with an invalid front end endpoint', function() {
            var error = 'Invalid front end endpoint specified (arg #1)';

            function createQueue(feEndpoint) {
                return function() {
                    return new SimpleQueue(feEndpoint);
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
            var error = 'Invalid back end endpoint specified (arg #2)';

            function createQueue(beEndpoint) {
                return function() {
                    return new SimpleQueue(feEndpoint, beEndpoint);
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

        it('should create an object that exposes members required by the interface', function() {
            _queue = _createQueue();

            expect(_queue).to.be.an.instanceof(_events.EventEmitter);
            expect(_queue).to.have.property('initialize').and.to.be.a('function');
            expect(_queue).to.have.property('dispose').and.to.be.a('function');
            expect(_queue).to.have.property('getPendingRequestCount').and.to.be.a('function');
            expect(_queue).to.have.property('getAvailableWorkerCount').and.to.be.a('function');
            expect(_queue).to.have.property('isReady').and.to.be.a('function');
        });

        it('should initialize properties to default values', function() {
            _queue = _createQueue();

            expect(_queue.getPendingRequestCount()).to.equal(0);
            expect(_queue.getAvailableWorkerCount()).to.equal(0);
            expect(_queue.isReady()).to.be.false;
        });
    });

    describe('initialize', function() {
        
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
                var client1 = _createReqSocket();

                client1.on('connect', function(){
                    def.resolve();
                });
                client1.connect(beEndpoint);

                return def.promise;
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });
    });

    describe('dispose', function() {
        it('should return a promise when invoked', function(done) {
            _queue = _createQueue();

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                var ret = _queue.dispose();

                expect(ret).to.be.an('object');
                expect(ret).to.have.property('then').and.to.be.a('function');
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

        function _connectAndSend(count, endpoint, message, addMsgNumber) {
            var promises = [];

            function getResolver(def, socket) {
                return function() {
                   def.resolve(socket); 
                };
            }

            for(var index=1; index<=count; index++) {
                var def = _q.defer();
                var socket = _createReqSocket();

                socket.connect(endpoint);
                if(addMsgNumber) {
                    message = message + ' #' + index.toString();
                }
                socket.send(message);
                socket.on('connect', getResolver(def, socket));

                promises.push(def.promise);
            }

            return _q.all(promises);
        }
        
        it('should increment the pending request count if a request is received, when no workers are available', function(done){
            var feEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(feEndpoint);

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                var count = 3;
                var promise = _connectAndSend(count, feEndpoint, 'MESSAGE');

                var doTests = _testUtils.getDelayedRunner(function() {
                    expect(_queue.getPendingRequestCount()).to.equal(count);
                }, DEFAULT_DELAY);

                return expect(promise).to.be.fulfilled.then(doTests);
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should increment the available workers count if a worker sends a message, when no requests are available', function(done){
            var beEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(null, beEndpoint);

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                var count = 3;
                var promise = _connectAndSend(count, beEndpoint, _messageDefinitions.READY);

                var doTests = _testUtils.getDelayedRunner(function() {
                    expect(_queue.getAvailableWorkerCount()).to.equal(count);
                }, DEFAULT_DELAY);

                return expect(promise).to.be.fulfilled.then(doTests);
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should raise the "REQUEST" event when a request is received from the client', function(done){
            var feEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(feEndpoint);

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                var client = _createReqSocket();
                client.identity = _uuid.v4();

                var clientMessage = 'MESSAGE';
                var def = _q.defer();

                _queue.on(_eventDefinitions.REQUEST, function(clientId, frames) {
                    _testUtils.runDeferred(function() {
                        expect(clientId.toString()).to.equal(client.identity);
                        expect(frames).to.have.length(3);
                        expect(frames[0].toString()).to.equal(client.identity);
                        expect(frames[1].toString()).to.be.empty;
                        expect(frames[2].toString()).to.equal(clientMessage);
                    }, def);
                });

                client.connect(feEndpoint);
                client.send(clientMessage);
                
                return def.promise;
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should raise the "ASSIGNED_REQUEST" event when a task has been assigned to a worker', function(done){
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(feEndpoint, beEndpoint);

            expect(_queue.initialize()).to.be.fulfilled.then(function() {
                var client = _createReqSocket();
                client.identity = _uuid.v4();
                var worker = _createReqSocket();
                worker.identity = _uuid.v4();

                var clientMessage = 'MESSAGE';
                var def = _q.defer();

                _queue.on(_eventDefinitions.ASSIGNED_REQUEST, function(workerId, frames) {
                    _testUtils.runDeferred(function() {
                        expect(workerId.toString()).to.equal(worker.identity);
                        expect(frames).to.have.length(5);
                        expect(frames[0].toString()).to.equal(worker.identity);
                        expect(frames[1].toString()).to.be.empty;
                        expect(frames[2].toString()).to.equal(client.identity);
                        expect(frames[3].toString()).to.be.empty;
                        expect(frames[4].toString()).to.equal(clientMessage);
                    }, def);
                });

                worker.connect(beEndpoint);
                worker.send(_messageDefinitions.READY);
                client.connect(feEndpoint);
                client.send(clientMessage);

                return def.promise;
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should raise the "ASSIGNED_RESPONSE" event when a response has been assigned to a client', function(done){
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(feEndpoint, beEndpoint);

            expect(_queue.initialize()).to.be.fulfilled.then(function() {
                var client = _createReqSocket();
                client.identity = _uuid.v4();
                var worker = _createReqSocket();
                worker.identity = _uuid.v4();
                var clientMessage = 'MESSAGE';
                var workerResponse = 'OK';
                var def = _q.defer();

                worker.on('message', function() {
                    var frames = Array.prototype.splice.call(arguments, 0);
                    worker.send([frames[0], frames[1], workerResponse]);
                });

                _queue.on(_eventDefinitions.ASSIGNED_RESPONSE, function(clientId, frames) {
                    _testUtils.runDeferred(function() {
                        expect(clientId.toString()).to.equal(client.identity);
                        expect(frames).to.have.length(3);
                        expect(frames[0].toString()).to.equal(client.identity);
                        expect(frames[1].toString()).to.be.empty;
                        expect(frames[2].toString()).to.equal(workerResponse);
                    }, def);
                });

                worker.connect(beEndpoint);
                worker.send(_messageDefinitions.READY);
                client.connect(feEndpoint);
                client.send(clientMessage);

                return def.promise;
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should immediately dispatch a new request to a worker if one is available', function(done){
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(feEndpoint, beEndpoint);

            expect(_queue.initialize()).to.be.fulfilled.then(function() {
                var workerCount = 3;
                var workerPromise = _connectAndSend(workerCount, beEndpoint, _messageDefinitions.READY);

                var connectClients = _testUtils.getDelayedRunner(function(){
                    var clientCount = 3;
                    var clientPromise = _connectAndSend(clientCount, feEndpoint, 'MESSAGE');
                    
                    var doTests = _testUtils.getDelayedRunner(function() {
                        expect(_queue.getAvailableWorkerCount()).to.equal(0);
                        expect(_queue.getPendingRequestCount()).to.equal(0);
                    }, DEFAULT_DELAY);

                    return expect(clientPromise).to.be.fulfilled.then(doTests);
                }, DEFAULT_DELAY);

                return expect(workerPromise).to.be.fulfilled.then(connectClients);
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should immediately provide a newly ready worker with a request if one is available', function(done){
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(feEndpoint, beEndpoint);

            expect(_queue.initialize()).to.be.fulfilled.then(function() {
                var clientCount = 3;
                var clientPromise = _connectAndSend(clientCount, feEndpoint, 'MESSAGE');

                var connectWorkers = _testUtils.getDelayedRunner(function() {
                    var workerCount = 3;
                    var workerPromise = _connectAndSend(workerCount, beEndpoint, _messageDefinitions.READY);
                    
                    var doTests = _testUtils.getDelayedRunner(function() {
                        expect(_queue.getAvailableWorkerCount()).to.equal(0);
                        expect(_queue.getPendingRequestCount()).to.equal(0);
                    }, DEFAULT_DELAY);

                    return expect(workerPromise).to.be.fulfilled.then(doTests);
                }, DEFAULT_DELAY);

                return expect(clientPromise).to.be.fulfilled.then(connectWorkers);
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should send the request to the worker in the correct format', function(done){
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            _queue = _createQueue(feEndpoint, beEndpoint);

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                var client = _createReqSocket();
                var worker = _createReqSocket();
                var clientMessage = 'MESSAGE';
                var def = _q.defer();

                worker.on('message', function() {
                    var frames = Array.prototype.splice.call(arguments, 0);
                    _testUtils.runDeferred(function() {
                        expect(frames).to.have.length(3);
                        expect(frames[1].toString()).to.equal('');
                        expect(frames[2].toString()).to.equal(clientMessage);
                    }, def);
                });

                worker.connect(beEndpoint);
                worker.send(_messageDefinitions.READY);
                client.connect(feEndpoint);
                client.send(clientMessage);

                return def.promise;
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should send replies from the worker back to the correct client', function(done) {
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            _queue = new SimpleQueue(feEndpoint, beEndpoint);

            function createEchoWorker() {
                var worker = _createReqSocket();
                worker.on('message', function() {
                    var frames = Array.prototype.splice.call(arguments, 0);
                    worker.send([frames[0], frames[1], 'OK', 'ECHO::' + frames[2].toString()]);
                });
                worker.connect(beEndpoint);
                worker.send(_messageDefinitions.READY);
            }

            function createClient(message) {
                var def = _q.defer();
                var client = _createReqSocket();
                client.on('message', function() {
                    var frames = Array.prototype.splice.call(arguments, 0);
                    def.resolve(frames);
                });
                client.connect(feEndpoint);
                client.send(message);

                return expect(def.promise).to.be.fulfilled.then(function(frames){
                    expect(frames).to.have.length(2);
                    expect(frames[0].toString()).to.equal('OK');
                    expect(frames[1].toString()).to.equal('ECHO::' + message);
                });
            }

            expect(_queue.initialize()).to.be.fulfilled.then(function(){
                var client1 = _createReqSocket();
                var client2 = _createReqSocket();

                createEchoWorker();
                createEchoWorker();

                var client1Promise = createClient('client1');
                var client2Promise = createClient('client2');

                return _q.all([
                    expect(client1Promise).to.be.fulfilled,
                    expect(client2Promise).to.be.fulfilled
                ]);
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

    });
});
