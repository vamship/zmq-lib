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
var SimpleBroker = require('../../lib/simple-broker');

describe('SimpleBroker', function() {
    var DEFAULT_DELAY = 10;
    var READY_MESSAGE = 'READY';

    var _broker;
    var _sockets;

    beforeEach(function(){
        _sockets = [];
    })

    afterEach(function(){
        if(_broker) {
            _broker.dispose();
        }
        _sockets.forEach(function(sock){
            sock.close();
        });
    });

    function _createBroker(feEndpoint, beEndpoint) {
        feEndpoint = feEndpoint || _testUtils.generateEndpoint();
        beEndpoint = beEndpoint || _testUtils.generateEndpoint();
        return new SimpleBroker(feEndpoint, beEndpoint);
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

            function createBroker(feEndpoint) {
                return function() {
                    return new SimpleBroker(feEndpoint);
                };
            }

            expect(createBroker()).to.throw(error);
            expect(createBroker(null)).to.throw(error);
            expect(createBroker('')).to.throw(error);
            expect(createBroker(1)).to.throw(error);
            expect(createBroker(true)).to.throw(error);
            expect(createBroker([])).to.throw(error);
            expect(createBroker({})).to.throw(error);
        });

        it('should throw an error if invoked with an invalid back end endpoint', function() {
            var feEndpoint = _testUtils.generateEndpoint();
            var error = 'Invalid back end endpoint specified (arg #2)';

            function createBroker(beEndpoint) {
                return function() {
                    return new SimpleBroker(feEndpoint, beEndpoint);
                };
            }

            expect(createBroker()).to.throw(error);
            expect(createBroker(null)).to.throw(error);
            expect(createBroker('')).to.throw(error);
            expect(createBroker(1)).to.throw(error);
            expect(createBroker(true)).to.throw(error);
            expect(createBroker([])).to.throw(error);
            expect(createBroker({})).to.throw(error);
        });

        it('should create an object that exposes members required by the interface', function() {
            _broker = _createBroker();

            expect(_broker).to.have.property('initialize').and.to.be.a('function');
            expect(_broker).to.have.property('dispose').and.to.be.a('function');
            expect(_broker).to.have.property('getPendingRequestCount').and.to.be.a('function');
            expect(_broker).to.have.property('getAvailableWorkerCount').and.to.be.a('function');
            expect(_broker).to.have.property('isReady').and.to.be.a('function');
        });

        it('should initialize properties to default values', function() {
            _broker = _createBroker();

            expect(_broker.getPendingRequestCount()).to.equal(0);
            expect(_broker.getAvailableWorkerCount()).to.equal(0);
            expect(_broker.isReady()).to.be.false;
        });
    });

    describe('initialize', function() {
        
        it('should return a promise when invoked', function(){
            _broker = _createBroker();

            var ret = _broker.initialize();
            expect(ret).to.be.an('object');
            expect(ret).to.have.property('then').and.to.be.a('function');
        });

        it('should reject the promise if the front end binding fails', function(done) {
            _broker = _createBroker('bad-endpoint');
            
            expect(_broker.initialize()).to.be.rejected.notify(done);
        });

        it('should reject the promise if the back end binding fails', function(done){
            _broker = _createBroker(null, 'bad-endpoint');

            expect(_broker.initialize()).to.be.rejected.notify(done);
        });

        it('should reject the promise if both front and back end binrings fail', function(done){
            _broker = _createBroker('bad-endpoint', 'bad-endpoint');

            expect(_broker.initialize()).to.be.rejected.notify(done);
        });

        it('should resolve the promise once both front and back ends have been bound successfully', function(done){
            _broker = _createBroker();

            expect(_broker.initialize()).to.be.fulfilled.notify(done);
        });

        it('should set isReady()=true when initialization succeeds', function(done) {
            _broker = _createBroker();

            expect(_broker.isReady()).to.be.false;

            expect(_broker.initialize()).to.be.fulfilled.then(function() {
                expect(_broker.isReady()).to.be.true;
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should set isReady()=false when initialization fails', function(done) {
            _broker = _createBroker('bad-endpoint', 'bad-endpoint');

            expect(_broker.isReady()).to.be.false;

            expect(_broker.initialize()).to.be.rejected.then(function() {
                expect(_broker.isReady()).to.be.false;
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should open a front end socket and bind to the endpoint when invoked', function(done) {
            var feEndpoint = _testUtils.generateEndpoint();
            _broker = _createBroker(feEndpoint);

            expect(_broker.initialize()).to.be.fulfilled.then(function(){
                var deferred = _q.defer();
                var client1 = _createReqSocket();

                client1.on('connect', function(){
                    deferred.resolve();
                });
                client1.connect(feEndpoint);

                return deferred.promise;
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should open a back end socket and bind to the endpoint when invoked', function(done) {
            var beEndpoint = _testUtils.generateEndpoint();
            _broker = _createBroker(beEndpoint);

            expect(_broker.initialize()).to.be.fulfilled.then(function(){
                var deferred = _q.defer();
                var client1 = _createReqSocket();

                client1.on('connect', function(){
                    deferred.resolve();
                });
                client1.connect(beEndpoint);

                return deferred.promise;
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });
    });

    describe('dispose', function() {
        it('should return a promise when invoked', function(done) {
            _broker = _createBroker();

            expect(_broker.initialize()).to.be.fulfilled.then(function(){
                var ret = _broker.dispose();

                expect(ret).to.be.an('object');
                expect(ret).to.have.property('then').and.to.be.a('function');
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should resolve the promise once the sockets have been successfully closed', function(done){
            _broker = _createBroker();

            expect(_broker.initialize()).to.be.fulfilled.then(function(){
                return expect(_broker.dispose()).to.be.fulfilled;
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should set isReady()=false when dispose succeeds', function(done) {
            _broker = _createBroker();

            expect(_broker.initialize()).to.be.fulfilled.then(function(){
                expect(_broker.isReady()).to.be.true;
                return expect(_broker.dispose()).to.be.fulfilled.then(function() {
                    expect(_broker.isReady()).to.be.false;
                });
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });
    });

    describe('[BROKER LOGIC]', function() {

        function _connectAndSend(count, endpoint, message, addMsgNumber) {
            var promises = [];

            function getResolver(deferred, socket) {
                return function() {
                   deferred.resolve(socket); 
                };
            }

            for(var index=1; index<=count; index++) {
                var deferred = _q.defer();
                var socket = _createReqSocket();

                socket.connect(endpoint);
                if(addMsgNumber) {
                    message = message + ' #' + index.toString();
                }
                socket.send(message);
                socket.on('connect', getResolver(deferred, socket));

                promises.push(deferred.promise);
            }

            return _q.all(promises);
        }
        
        it('should increment the pending request count if a request is received, when no workers are available', function(done){
            var feEndpoint = _testUtils.generateEndpoint();
            _broker = _createBroker(feEndpoint);

            expect(_broker.initialize()).to.be.fulfilled.then(function(){
                var count = 3;
                var promise = _connectAndSend(count, feEndpoint, 'MESSAGE');

                var doTests = _testUtils.getDelayedRunner(function() {
                    expect(_broker.getPendingRequestCount()).to.equal(count);
                }, DEFAULT_DELAY);

                return expect(promise).to.be.fulfilled.then(doTests);
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should increment the available workers count if a worker sends a message, when no requests are available', function(done){
            var beEndpoint = _testUtils.generateEndpoint();
            _broker = _createBroker(null, beEndpoint);

            expect(_broker.initialize()).to.be.fulfilled.then(function(){
                var count = 3;
                var promise = _connectAndSend(count, beEndpoint, 'READY');

                var doTests = _testUtils.getDelayedRunner(function() {
                    expect(_broker.getAvailableWorkerCount()).to.equal(count);
                }, DEFAULT_DELAY);

                return expect(promise).to.be.fulfilled.then(doTests);
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should immediately dispatch a new request to a worker if one is available', function(done){
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            _broker = _createBroker(feEndpoint, beEndpoint);

            expect(_broker.initialize()).to.be.fulfilled.then(function() {
                var workerCount = 3;
                var workerPromise = _connectAndSend(workerCount, beEndpoint, 'READY');

                var connectClients = _testUtils.getDelayedRunner(function(){
                    var clientCount = 3;
                    var clientPromise = _connectAndSend(clientCount, feEndpoint, 'MESSAGE');
                    
                    var doTests = _testUtils.getDelayedRunner(function() {
                        expect(_broker.getAvailableWorkerCount()).to.equal(0);
                        expect(_broker.getPendingRequestCount()).to.equal(0);
                    }, DEFAULT_DELAY);

                    return expect(clientPromise).to.be.fulfilled.then(doTests);
                }, DEFAULT_DELAY);

                return expect(workerPromise).to.be.fulfilled.then(connectClients);
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should immediately provide a newly ready worker with a request if one is available', function(done){
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            _broker = _createBroker(feEndpoint, beEndpoint);

            expect(_broker.initialize()).to.be.fulfilled.then(function() {
                var clientCount = 3;
                var clientPromise = _connectAndSend(clientCount, feEndpoint, 'MESSAGE');

                var connectWorkers = _testUtils.getDelayedRunner(function() {
                    var workerCount = 3;
                    var workerPromise = _connectAndSend(workerCount, beEndpoint, 'READY');
                    
                    var doTests = _testUtils.getDelayedRunner(function() {
                        expect(_broker.getAvailableWorkerCount()).to.equal(0);
                        expect(_broker.getPendingRequestCount()).to.equal(0);
                    }, DEFAULT_DELAY);

                    return expect(workerPromise).to.be.fulfilled.then(doTests);
                }, DEFAULT_DELAY);

                return expect(clientPromise).to.be.fulfilled.then(connectWorkers);
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should send the request to the worker in the correct format', function(done){
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            _broker = _createBroker(feEndpoint, beEndpoint);

            expect(_broker.initialize()).to.be.fulfilled.then(function(){
                var client = _createReqSocket();
                var worker = _createReqSocket();
                var clientMessage = 'MESSAGE';
                var deferred = _q.defer();

                worker.on('message', function() {
                    var frames = Array.prototype.splice.call(arguments, 0);
                    _testUtils.runDeferred(function() {
                        expect(frames).to.have.length(3);
                        expect(frames[1].toString()).to.equal('');
                        expect(frames[2].toString()).to.equal(clientMessage);
                    }, deferred);
                });

                worker.connect(beEndpoint);
                worker.send(READY_MESSAGE);
                client.connect(feEndpoint);
                client.send(clientMessage);

                return deferred.promise;
            }).then(_getSuccessCallback(done), _getFailureCallback(done));
        });

        it('should send replies from the worker back to the correct client', function(done) {
            var feEndpoint = _testUtils.generateEndpoint();
            var beEndpoint = _testUtils.generateEndpoint();
            _broker = new SimpleBroker(feEndpoint, beEndpoint);

            function createEchoWorker() {
                var worker = _createReqSocket();
                worker.on('message', function() {
                    var frames = Array.prototype.splice.call(arguments, 0);
                    worker.send([frames[0], frames[1], 'ECHO::' + frames[2].toString()]);
                });
                worker.connect(beEndpoint);
                worker.send(READY_MESSAGE);
            }

            function createClient(message) {
                var deferred = _q.defer();
                var client = _createReqSocket();
                client.on('message', function() {
                    var frames = Array.prototype.splice.call(arguments, 0);
                    deferred.resolve(frames);
                });
                client.connect(feEndpoint);
                client.send(message);

                return expect(deferred.promise).to.be.fulfilled.then(function(frames){
                    expect(frames).to.have.length(1);
                    expect(frames[0].toString()).to.equal('ECHO::' + message);
                });
            }

            expect(_broker.initialize()).to.be.fulfilled.then(function(){
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
