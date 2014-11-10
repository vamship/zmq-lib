/* jshint node:true, expr:true */
'use strict';

var _events = require('events');
var _zmq = require('zmq');
var _sinon = require('sinon');
var _sinonChai = require('sinon-chai');
var _chai = require('chai');
_chai.use(_sinonChai);

var expect = _chai.expect;
var Monitor = require('../../lib/monitor');
var LazyPirateClient = require('../../lib/lazy-pirate-client');

describe('LazyPirateClient', function() {
    var DEFAULT_ENDPOINT = 'ipc://server';
    var DEFAULT_RETRY_FREQ = 2500;
    var DEFAULT_RETRY_COUNT = 3;
    var _repSock;
    var _client;

    function _createRepSocket(endpoint) {
        endpoint = endpoint || DEFAULT_ENDPOINT;
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

    function _createLpClient(monitor) {
        monitor = monitor || new Monitor(DEFAULT_RETRY_FREQ, DEFAULT_RETRY_COUNT);
        var socket = new LazyPirateClient(DEFAULT_ENDPOINT, monitor);

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
                return new LazyPirateClient(endpoint);
            }

            expect(function() { createClient() }).to.throw(error);
            expect(function() { createClient(null) }).to.throw(error);
            expect(function() { createClient('') }).to.throw(error);
            expect(function() { createClient(1) }).to.throw(error);
            expect(function() { createClient(true) }).to.throw(error);
            expect(function() { createClient([]) }).to.throw(error);
            expect(function() { createClient({}) }).to.throw(error);
        });

        it('should throw an error if a valid retry monitor object is not specified', function() {
            var error = 'Invalid retry monitor specified (arg #2)';

            function createClient(monitor) {
                return new LazyPirateClient(DEFAULT_ENDPOINT, monitor);
            }

            expect(function() { createClient() }).to.throw(error);
            expect(function() { createClient(null) }).to.throw(error);
            expect(function() { createClient('') }).to.throw(error);
            expect(function() { createClient(1) }).to.throw(error);
            expect(function() { createClient(true) }).to.throw(error);
            expect(function() { createClient([]) }).to.throw(error);
            expect(function() { createClient({}) }).to.throw(error);
        });

        it('should create an object that exposes members required by the interface', function() {
            var monitor = new Monitor(2500, 3);
            var client = new LazyPirateClient(DEFAULT_ENDPOINT, monitor);

            expect(client).to.be.an('object');
            expect(client).to.be.an.instanceof(_events.EventEmitter);
            expect(client).to.have.property('initialize').and.to.be.a('function');
            expect(client).to.have.property('send').and.to.be.a('function');
            expect(client).to.have.property('dispose').and.to.be.a('function');
            expect(client).to.have.property('isReady').and.to.be.a('function');
        });

        it('should set property values to defaults', function() {
            var monitor = new Monitor(2500, 3);
            var client = new LazyPirateClient(DEFAULT_ENDPOINT, monitor);

            expect(client.isReady()).to.be.false;
        });
    });

    describe('initialize()', function() {
        it('should initialize a connection to a peer endpoint when invoked', function(done) {
            _client = _createLpClient();
            _repSock = _createRepSocket();

            _repSock.on('accept', function() {
                // If this event is not triggered, the test will timeout and fail.
                done();
            });
            
            _client.initialize();
        });

        it('should raise a "ready" event with null args after initializing the object', function() {
            var handlerSpy = _sinon.spy();

            _client = _createLpClient();
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

            _client = _createLpClient();

            expect(function() { _client.send('foo'); }).to.throw(error);
        });

        it('should send a message over a zero mq socket when invoked', function(done) {
            var clientMessage = 'hello';

            _repSock = _createRepSocket();
            _client = _createLpClient();
            
            _repSock.on('message', function(message) {
                expect(message.toString()).to.equal(clientMessage);
                done();
            });

            _client.on('ready', function() {
                _client.send(clientMessage);
            });

            _client.initialize();
        });

        it('should throw an error if an attempt is made to send data when the socket is waiting for a response', function(done) {
            var error = 'Cannot send message. The socket is still waiting for a response from a previous request';
            var clientMessage = 'hello';
            
            _repSock = _createRepSocket();
            _client = _createLpClient();

            _client.on('ready', function() {
                _client.send(clientMessage);
                expect(function() {
                    _client.send(clientMessage);
                }).to.throw(error);

                done();
            });

            _client.initialize();
        });

        it('should raise a "ready" event once a response is received to a request', function(done) {
            var clientMessage = 'hello';

            _repSock = _createRepSocket();
            _client = _createLpClient();
            
            _repSock.on('message', function(message) {
                _repSock.send('OK');
            });

            var eventCount = 0;
            _client.on('ready', function() {
                eventCount++;

                if(eventCount > 1) {
                    // If this condition is not met, the test will timeout and fail.
                    done();
                } else {
                    _client.send(clientMessage);
                }
            });

            _client.initialize();
        });

        it('should set isReady()=false once invoked.', function(done) {
            var clientMessage = 'hello';
            
            _repSock = _createRepSocket();
            _client = _createLpClient();

            _client.on('ready', function() {
                expect(_client.isReady()).to.be.true;
                _client.send(clientMessage);
                expect(_client.isReady()).to.be.false;

                done();
            });

            _client.initialize();
        });

        it('should set isReady()=true once a response is received to a request', function(done) {
            var clientMessage = 'hello';
            
            _repSock = _createRepSocket();
            _client = _createLpClient();

            _repSock.on('message', function(message) {
                _repSock.send('OK');
            });

            var eventCount = 0;
            _client.on('ready', function() {
                eventCount++;

                if(eventCount > 1) {
                    expect(_client.isReady()).to.be.true;
                    done();
                } else {
                    _client.send(clientMessage);
                }
            });

            _client.initialize();
        });

        describe('[RETRY LOGIC]', function() {
            var RETRY_FREQUENCY = 100;
            var RETRY_COUNT = 3;

            it('should retry a request if a response is not received after a specified duration', function(done){
                var clientMessage = 'hello';
                var messageCounter = 0;

                function initRepSocket() {
                    _repSock = _createRepSocket();
                    _repSock.on('message', function(message){
                        messageCounter++;
                        if(messageCounter > 1) {
                            // If this condition is not met, the test will timeout and fail.
                            done();
                        } else {
                            // Reinitialize the rep socket, effectively dropping the request.
                            initRepSocket();
                        }
                    });
                }

                initRepSocket();
                _client = _createLpClient(new Monitor(200, 3));

                _client.on('ready', function() {
                    _client.send(clientMessage);
                });

                _client.initialize();
            });

            it('should abandon retries after a specified number of retries have failed', function(done){
                var clientMessage = 'hello';
                var messageCounter = 0;
                var retryCount = 3;

                function initRepSocket() {
                    _repSock = _createRepSocket();
                    _repSock.on('message', function(message) {
                        messageCounter++;
                        initRepSocket();
                    });
                }

                initRepSocket();
                _client = _createLpClient(new Monitor(200, retryCount));

                _client.on('ready', function() {
                    _client.send(clientMessage);
                });

                _client.on('abandoned', function() {
                    expect(messageCounter).to.be.at.least(retryCount);
                    done();
                });

                _client.initialize();
            });
        });
    });
    
    describe('dispose()', function() {
        it('should close an open socket when invoked.', function(done) {
            var connectionMade = false;
            _repSock = _createRepSocket();
            _client = _createLpClient();

            _repSock.on('disconnect', function(message) {

                //Expect that the connection was live prior to the disconnect.
                expect(connectionMade).to.be.true;
                done();
            });

            _client.on('ready', function() {
                connectionMade = true;
                _client.dispose();
            });
            _client.initialize();
        });

        it('should set isReady()=false when invoked.', function(done) {
            var isReady = false;
            _repSock = _createRepSocket();
            _client = _createLpClient();

            _repSock.on('disconnect', function(message) {

                //Expect that the client was ready before disconnect, and that
                //it is no longer ready after disconnect.
                expect(isReady).to.be.true;
                expect(_client.isReady()).to.be.false;
                done();
            });

            _client.on('ready', function() {
                isReady = true;
                _client.dispose();
            });
            _client.initialize();
        });
    });
});
