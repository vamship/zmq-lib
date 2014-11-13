/* jshint node:true, expr:true */
'use strict';

var _events = require('events');
var _zmq = require('zmq');
var _q = require('q');
var _sinon = require('sinon');
var _sinonChai = require('sinon-chai');
var _chaiAsPromised = require('chai-as-promised');
var _chai = require('chai');
_chai.use(_sinonChai);
_chai.use(_chaiAsPromised);

var expect = _chai.expect;
var _testUtils = require('../test-util');
var SimpleBroker = require('../../lib/simple-broker');

describe('SimpleBroker', function() {
    var DEFAULT_FE_ENDPOINT = 'ipc://fe';
    var DEFAULT_BE_ENDPOINT = 'ipc://be';
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

    function _createRepSocket() {
        var socket =  _zmq.createSocket('req');
        socket.monitor(10);
        _sockets.push(socket);
        
        return socket;
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
            var error = 'Invalid back end endpoint specified (arg #2)';

            function createBroker(beEndpoint) {
                return function() {
                    return new SimpleBroker(DEFAULT_FE_ENDPOINT, beEndpoint);
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
            _broker = new SimpleBroker(DEFAULT_FE_ENDPOINT, DEFAULT_BE_ENDPOINT);

            expect(_broker).to.have.property('initialize').and.to.be.a('function');
            expect(_broker).to.have.property('dispose').and.to.be.a('function');
            expect(_broker).to.have.property('getPendingRequestCount').and.to.be.a('function');
            expect(_broker).to.have.property('getAvailableWorkerCount').and.to.be.a('function');
            expect(_broker).to.have.property('isReady').and.to.be.a('function');
        });

        it('should initialize properties to default values', function() {
            _broker = new SimpleBroker(DEFAULT_FE_ENDPOINT, DEFAULT_BE_ENDPOINT);

            expect(_broker.getPendingRequestCount()).to.equal(0);
            expect(_broker.getAvailableWorkerCount()).to.equal(0);
            expect(_broker.isReady()).to.be.false;
        });
    });

    describe('initialize', function() {
        
        it('should return a promise when invoked', function(){
            _broker = new SimpleBroker(DEFAULT_FE_ENDPOINT, DEFAULT_BE_ENDPOINT);

            var ret = _broker.initialize();
            expect(ret).to.be.an('object');
            expect(ret).to.have.property('then').and.to.be.a('function');
        });

        it('should reject the promise if the front end binding fails', function(){
            _broker = new SimpleBroker('bad-endpoint', DEFAULT_BE_ENDPOINT);
            
            expect(_broker.initialize()).to.be.rejected;
        });

        it('should reject the promise if the back end binding fails', function(){
            _broker = new SimpleBroker(DEFAULT_FE_ENDPOINT, 'bad-endpoint');

            expect(_broker.initialize()).to.be.rejected;
        });

        it('should reject the promise if both front and back end bindings fail', function(){
            _broker = new SimpleBroker('bad-endpoint', 'bad-endpoint');

            expect(_broker.initialize()).to.be.rejected;
        });

        it('should resolve the promise once both front and back ends have been bound successfully', function(){
            _broker = new SimpleBroker(DEFAULT_FE_ENDPOINT, DEFAULT_BE_ENDPOINT);

            expect(_broker.initialize()).to.be.fulfilled;
        });


        it('should set isReady()=true when initialization succeeds', function(done) {
            _broker = new SimpleBroker(DEFAULT_FE_ENDPOINT, DEFAULT_BE_ENDPOINT);

            expect(_broker.isReady()).to.be.false;

            var promise = _broker.initialize();
            expect(promise).to.be.fulfilled;

            promise.then(function success(results) {
                _testUtils.evaluateExpectations(function(){
                    expect(_broker.isReady()).to.be.true;
                }, done);
            });
        });

        it('should set isReady()=false when initialization fails', function(done) {
            _broker = new SimpleBroker('bad-endpoint', 'bad-endpoint');

            expect(_broker.isReady()).to.be.false;

            var promise = _broker.initialize();
            expect(promise).to.be.rejected;

            promise.then(null, function fail(results) {
                _testUtils.evaluateExpectations(function(){
                    expect(_broker.isReady()).to.be.false;
                }, done);
            });
        });

        it('should open a front end socket and bind to the endpoint when invoked', function(done) {
            _broker = new SimpleBroker(DEFAULT_FE_ENDPOINT, DEFAULT_BE_ENDPOINT);

            _broker.initialize().then(function success(){
                var client1 = _createRepSocket();

                client1.on('connect', function(){
                    done();
                });

                client1.connect(DEFAULT_FE_ENDPOINT);
            });
        });

        it('should open a back end socket and bind to the endpoint when invoked', function(done) {
            // This test seems to be taking a little longer than usual to execute. The
            // delay is intermittent, and the timeout setting is a band aid to get the 
            // tests to work consistently.
            this.timeout(5000);

            _broker = new SimpleBroker(DEFAULT_FE_ENDPOINT, DEFAULT_BE_ENDPOINT);

            _broker.initialize().then(function success(){
                var client1 = _createRepSocket();

                client1.on('connect', function(){
                    done();
                });

                client1.connect(DEFAULT_BE_ENDPOINT);
            });
        });
    });

    describe('dispose', function() {
        it('should return a promise when invoked', function(done) {
            _broker = new SimpleBroker(DEFAULT_FE_ENDPOINT, DEFAULT_BE_ENDPOINT);

            _broker.initialize().then(function success(){
                var ret = _broker.dispose();

                _testUtils.evaluateExpectations(function() {
                    expect(ret).to.be.an('object');
                    expect(ret).to.have.property('then').and.to.be.a('function');
                }, done);
            });
        });

        it('should resolve the promise once the sockets have been successfully closed', function(done){
            _broker = new SimpleBroker(DEFAULT_FE_ENDPOINT, DEFAULT_BE_ENDPOINT);

            _broker.initialize().then(function initSuccess(){
                _broker.dispose().then(function disposeSuccess(){
                    done();
                });
            });
        });

        it('should set isReady()=false when dispose succeeds', function(done) {
            _broker = new SimpleBroker(DEFAULT_FE_ENDPOINT, DEFAULT_BE_ENDPOINT);

            _broker.initialize().then(function initSuccess(){
                expect(_broker.isReady()).to.be.true;
                _broker.dispose().then(function disposeSuccess(){
                    _testUtils.evaluateExpectations(function() {
                        expect(_broker.isReady()).to.be.false;
                    }, done);
                });
            });
        });
    });

    describe('[BROKER LOGIC]', function() {

        function _connectAndSend(count, endpoint, message) {
            for(var index=0; index<count; index++) {
                var socket = _createRepSocket();
                socket.connect(endpoint);
                socket.send(message);
            }
        }
        
        function _waitAndCheck(runExpectations, done, finish, delay) {
            var deferred = _q.defer();
            delay = delay || 1;
            setTimeout(function() {
                _testUtils.evaluateExpectations(runExpectations, done, !finish);
                deferred.resolve();
            }, delay);

            return deferred.promise;
        }

        it('should increment the pending request count if a request is received, when no workers are available', function(done){
            _broker = new SimpleBroker(DEFAULT_FE_ENDPOINT, DEFAULT_BE_ENDPOINT);

            _broker.initialize().then(function success(){
                var count = 3;
                _connectAndSend(count, DEFAULT_FE_ENDPOINT, 'MESSAGE');

                //Give the broker some time to pick up and process the messages.
                _waitAndCheck(function() {
                    expect(_broker.getPendingRequestCount()).to.equal(count);
                }, done, true);
            });
        });

        it('should increment the available workers count everytime a new worker sends a READY message, when no requests are available', function(done){
            _broker = new SimpleBroker(DEFAULT_FE_ENDPOINT, DEFAULT_BE_ENDPOINT);

            _broker.initialize().then(function success(){
                var count = 3;
                _connectAndSend(count, DEFAULT_BE_ENDPOINT, 'READY');

                //Give the broker some time to pick up and process the messages.
                _waitAndCheck(function() {
                    expect(_broker.getAvailableWorkerCount()).to.equal(count);
                }, done, true);
            });
        });

        it('should immediately dispatch a new request to a worker if one is available', function(done){
            _broker = new SimpleBroker(DEFAULT_FE_ENDPOINT, DEFAULT_BE_ENDPOINT);

            _broker.initialize().then(function success() {
                var workerCount = 3;
                _connectAndSend(workerCount, DEFAULT_BE_ENDPOINT, 'READY');

                //Give the broker some time to pick up and process the messages.
                _waitAndCheck(function() {
                    expect(_broker.getAvailableWorkerCount()).to.equal(workerCount);
                }, done).then(function(){
                    var clientCount = 3;
                    _connectAndSend(clientCount, DEFAULT_FE_ENDPOINT, 'MESSAGE');
                    _waitAndCheck(function() {
                        expect(_broker.getAvailableWorkerCount()).to.equal(0);
                        expect(_broker.getPendingRequestCount()).to.equal(0);
                    }, done, true, 1);
                });
            });
        });

        it('should immediately provide a newly ready worker with a request if one is available', function(done){
            _broker = new SimpleBroker(DEFAULT_FE_ENDPOINT, DEFAULT_BE_ENDPOINT);

            _broker.initialize().then(function success() {
                var clientCount = 3;
                _connectAndSend(clientCount, DEFAULT_FE_ENDPOINT, 'MESSAGE');

                //Give the broker some time to pick up and process the messages.
                _waitAndCheck(function() {
                    expect(_broker.getPendingRequestCount()).to.equal(clientCount);
                }, done).then(function(){
                    var workerCount = 3;
                    _connectAndSend(workerCount, DEFAULT_BE_ENDPOINT, 'READY');
                    _waitAndCheck(function() {
                        expect(_broker.getAvailableWorkerCount()).to.equal(0);
                        expect(_broker.getPendingRequestCount()).to.equal(0);
                    }, done, true, 1);
                });
            });

        });

    });
});
