/* jshint node:true, expr:true */
'use strict';

var _events = require('events');
var _q = require('q');
var _zmq = require('zmq');
var _sinon = require('sinon');
var _chai = require('chai');
_chai.use(require('sinon-chai'));
_chai.use(require('chai-as-promised'));

var expect = _chai.expect;
var _testUtils = require('../test-util');
var Monitor = require('../../lib/monitor');
var LazyPirateClient = require('../../lib/lazy-pirate-client');

describe('LazyPirateClient', function() {
    var DEFAULT_RETRY_FREQ = 2500;
    var DEFAULT_RETRY_COUNT = 3;
    var _repSock;
    var _client;

    function _createRepSocket(endpoint) {
        var socket = _zmq.createSocket('rep');
        socket.monitor(10);
        socket.bind(endpoint);

        return socket;
    }

    function _destroyReqSocket(reqSock) {
        if(reqSock){
            reqSock.dispose();
        }
    }

    function _createLpClient(endpoint, monitor) {
        monitor = monitor || new Monitor(DEFAULT_RETRY_FREQ, DEFAULT_RETRY_COUNT);
        var socket = new LazyPirateClient(endpoint, monitor);

        return socket;
    }

    function _destroyLpClient(client) {
        if(client){
            client.close();
        }
    }

    afterEach(function() {
        // Clean up resources. This will happen even if tests fail.
        _destroyReqSocket(_client);
        _destroyLpClient(_repSock);
        _client = null;
        _repSock = null;
    });

    describe('ctor()', function() {
        it('should throw an error if a valid endpoint value is not specified', function() {
            var error = 'Invalid endpoint specified (arg #1)';

            function createClient(endpoint) {
                return function() {
                    return new LazyPirateClient(endpoint);
                };
            }

            expect(createClient()).to.throw(error);
            expect(createClient(null)).to.throw(error);
            expect(createClient('')).to.throw(error);
            expect(createClient(1)).to.throw(error);
            expect(createClient(true)).to.throw(error);
            expect(createClient([])).to.throw(error);
            expect(createClient({})).to.throw(error);
        });

        it('should throw an error if a valid retry monitor object is not specified', function() {
            var error = 'Invalid retry monitor specified (arg #2)';

            function createClient(monitor) {
                return function() {
                    return new LazyPirateClient(_testUtils.generateEndpoint(), monitor);
                };
            }

            expect(createClient()).to.throw(error);
            expect(createClient(null)).to.throw(error);
            expect(createClient('')).to.throw(error);
            expect(createClient(1)).to.throw(error);
            expect(createClient(true)).to.throw(error);
            expect(createClient([])).to.throw(error);
            expect(createClient({})).to.throw(error);
        });

        it('should create an object that exposes members required by the interface', function() {
            var monitor = new Monitor(2500, 3);
            var client = new LazyPirateClient(_testUtils.generateEndpoint(), monitor);

            expect(client).to.be.an('object');
            expect(client).to.be.an.instanceof(_events.EventEmitter);
            expect(client).to.have.property('initialize').and.to.be.a('function');
            expect(client).to.have.property('send').and.to.be.a('function');
            expect(client).to.have.property('dispose').and.to.be.a('function');
            expect(client).to.have.property('isReady').and.to.be.a('function');
        });

        it('should set property values to defaults', function() {
            var monitor = new Monitor(2500, 3);
            var client = new LazyPirateClient(_testUtils.generateEndpoint(), monitor);

            expect(client.isReady()).to.be.false;
        });
    });

    describe('initialize()', function() {
        it('should initialize a connection to a peer endpoint when invoked', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();

            _client = _createLpClient(endpoint);
            _repSock = _createRepSocket(endpoint);

            _repSock.on('accept', function() {
                // If this event is not triggered, the test will timeout and fail.
                def.resolve();
            });
            expect(def.promise).to.be.fulfilled.notify(done);
            
            _client.initialize();
        });

        it('should raise a "ready" event with null args after initializing the object', function() {
            var handlerSpy = _sinon.spy();
            var endpoint = _testUtils.generateEndpoint();

            _client = _createLpClient(endpoint);
            _client.on('ready', handlerSpy);

            expect(handlerSpy).to.not.have.been.called;
            _client.initialize();
            expect(handlerSpy).to.have.been.called;
            expect(handlerSpy).to.have.been.calledWithExactly(null);
        });
    });

    describe('send()', function() {
        it('should throw an error if invoked before the socket has been initialized', function() {
            var error = 'Socket not initialized. Cannot send message';
            var endpoint = _testUtils.generateEndpoint();

            _client = _createLpClient(endpoint);

            expect(function() { _client.send('foo'); }).to.throw(error);
        });

        it('should send a message over a zero mq socket when invoked', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();
            var clientMessage = 'hello';

            _repSock = _createRepSocket(endpoint);
            _client = _createLpClient(endpoint);

            _repSock.on('message', function(message) {
                _testUtils.runDeferred(function(){
                    expect(message.toString()).to.equal(clientMessage);
                }, def);
            });

            _client.on('ready', function() {
                _client.send(clientMessage);
            });

            _client.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should throw an error if an attempt is made to send data when the socket is waiting for a response', function(done) {
            var def = _q.defer();
            var error = 'Cannot send message. The socket is still waiting for a response from a previous request';
            var clientMessage = 'hello';
            var endpoint = _testUtils.generateEndpoint();
            
            _repSock = _createRepSocket(endpoint);
            _client = _createLpClient(endpoint);

            _client.on('ready', function() {
                _client.send(clientMessage);
                _testUtils.runDeferred(function() {
                    expect(function() {
                        _client.send(clientMessage);
                    }).to.throw(error);
                }, def);
            });

            _client.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should raise a "ready" event once a response is received to a request', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();
            var clientMessage = 'hello';

            _repSock = _createRepSocket(endpoint);
            _client = _createLpClient(endpoint);
            
            _repSock.on('message', function(message) {
                _repSock.send('OK');
            });

            var eventCount = 0;
            _client.on('ready', function() {
                eventCount++;

                if(eventCount > 1) {
                    // If this condition is not met, the test will timeout and fail.
                    def.resolve();
                } else {
                    _client.send(clientMessage);
                }
            });

            _client.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should set isReady()=false once invoked.', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();
            var clientMessage = 'hello';
            
            _repSock = _createRepSocket(endpoint);
            _client = _createLpClient(endpoint);

            _client.on('ready', function() {
                _testUtils.runDeferred(function() {
                    expect(_client.isReady()).to.be.true;
                    _client.send(clientMessage);
                    expect(_client.isReady()).to.be.false;
                }, def);
            });

            _client.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should set isReady()=true once a response is received to a request', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();
            var clientMessage = 'hello';
            
            _repSock = _createRepSocket(endpoint);
            _client = _createLpClient(endpoint);

            _repSock.on('message', function(message) {
                _repSock.send('OK');
            });

            var eventCount = 0;
            _client.on('ready', function() {
                eventCount++;

                if(eventCount > 1) {
                    _testUtils.runDeferred(function() {
                        expect(_client.isReady()).to.be.true;
                    }, def);
                } else {
                    _client.send(clientMessage);
                }
            });

            _client.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        describe('[RETRY LOGIC]', function() {
            var RETRY_FREQUENCY = 100;
            var RETRY_COUNT = 3;

            it('should retry a request if a response is not received after a specified duration', function(done){
                var def = _q.defer();
                var endpoint = _testUtils.generateEndpoint();
                var clientMessage = 'hello';
                var messageCounter = 0;

                function initRepSocket() {
                    _repSock = _createRepSocket(endpoint);
                    _repSock.on('message', function(message){
                        messageCounter++;
                        if(messageCounter > 1) {
                            // If this condition is not met, the test will timeout and fail.
                            def.resolve();
                        } else {
                            // Reinitialize the rep socket, effectively dropping the request.
                            initRepSocket();
                        }
                    });
                }

                initRepSocket();
                _client = _createLpClient(endpoint, new Monitor(200, 3));

                _client.on('ready', function() {
                    _client.send(clientMessage);
                });

                _client.initialize();

                expect(def.promise).to.be.fulfilled.notify(done);
            });

            it('should abandon retries after a specified number of retries have failed', function(done){
                var def = _q.defer();
                var endpoint = _testUtils.generateEndpoint();
                var clientMessage = 'hello';
                var messageCounter = 0;
                var retryCount = 3;

                function initRepSocket() {
                    _repSock = _createRepSocket(endpoint);
                    _repSock.on('message', function(message) {
                        messageCounter++;
                        initRepSocket();
                    });
                }

                initRepSocket();
                _client = _createLpClient(endpoint, new Monitor(200, retryCount));

                _client.on('ready', function() {
                    _client.send(clientMessage);
                });

                _client.on('abandoned', function() {
                    _testUtils.runDeferred(function() {
                        expect(messageCounter).to.be.at.least(retryCount);
                    }, def);
                });

                _client.initialize();

                expect(def.promise).to.be.fulfilled.notify(done);
            });
        });
    });
    
    describe('dispose()', function() {
        it('should close an open socket, and set isReady()=false when invoked.', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();
            var connectionMade = false;
            var wasReady = false;

            _repSock = _createRepSocket(endpoint);
            _client = _createLpClient(endpoint);

            _repSock.on('disconnect', function(message) {
                _testUtils.runDeferred(function() {

                    //Expect that the connection was live prior to the disconnect.
                    expect(connectionMade).to.be.true;

                    //Expect that the client was ready before disconnect, and that
                    //it is no longer ready after disconnect.
                    expect(wasReady).to.be.true;
                    expect(_client.isReady()).to.be.false;
                }, def);
            });

            _client.on('ready', function() {
                connectionMade = true;
                wasReady = _client.isReady();
                _client.dispose();
            });

            _client.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });
    });
});
