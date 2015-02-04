/* jshint node:true, expr:true */
'use strict';

var _events = require('events');
var _q = require('q');
var _zmq = require('zmq');
var _sinon = require('sinon');
var _chai = require('chai');
_chai.use(require('sinon-chai'));
_chai.use(require('chai-as-promised'));

var Monitor = require('../../lib/monitor');
var LazyPiratePeer = require('../../lib/lazy-pirate-peer');
var _messageDefinitions = require('../../lib/message-definitions');
var _eventDefinitions = require('../../lib/event-definitions');

var _testUtil = require('../test-util');
var expect = _chai.expect;

describe('LazyPiratePeer', function() {
    var DEFAULT_POLL_FREQUENCY = 2500;
    var DEFAULT_REQUEST_TIMEOUT = 500;
    var DEFAULT_RETRY_COUNT = 3;
    var DEFAULT_DELAY = 600;
    var _peer;
    var _client;

    function _createPeer(endpoint, isServer) {
        var socket = _zmq.createSocket('pair');
        socket.monitor(10);
        if(isServer) {
            socket.bind(endpoint);
        } else {
            socket.connect(endpoint);
        }

        return socket;
    }

    function _createLPClient(endpoint, monitor, options) {
        options = options || {};
        options.requestTimeout = options.requestTimeout || DEFAULT_REQUEST_TIMEOUT;
        options.retryCount = options.retryCount || DEFAULT_RETRY_COUNT;

        monitor = monitor || new Monitor(DEFAULT_POLL_FREQUENCY, -1);
        var client = new LazyPiratePeer(endpoint, monitor, options);

        return client;
    }

    afterEach(function() {
        // Clean up resources. This will happen even if tests fail.
        if(_client){
            _client.dispose();
        }
        if(_peer){
            _peer.close();
        }
        _client = null;
        _peer = null;
    });

    describe('ctor()', function() {
        it('should throw an error if a valid endpoint value is not specified', function() {
            var error = 'Invalid endpoint specified (arg #1)';

            function createClient(endpoint) {
                return function() {
                    return new LazyPiratePeer(endpoint);
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
                    return new LazyPiratePeer(_testUtil.generateEndpoint(), monitor);
                };
            }

            expect(createClient()).to.throw(error);
            expect(createClient(null)).to.throw(error);
            expect(createClient('abc')).to.throw(error);
            expect(createClient(1)).to.throw(error);
            expect(createClient(true)).to.throw(error);
            expect(createClient([])).to.throw(error);
            expect(createClient({})).to.throw(error);
        });

        it('should throw an error if a valid options object is not specified', function() {
            var error = 'Invalid options specified (arg #3)';

            function createClient(options) {
                return function() {
                    var monitor = new Monitor(2500, 3);
                    return new LazyPiratePeer(_testUtil.generateEndpoint(), monitor, options);
                };
            }

            expect(createClient()).to.throw(error);
            expect(createClient(null)).to.throw(error);
            expect(createClient('abc')).to.throw(error);
            expect(createClient(1)).to.throw(error);
            expect(createClient(true)).to.throw(error);
        });

        it('should throw an error if the options object does not define a valid request timeout', function() {
            var error = 'Options does not define a valid request timeout value (options.requestTimeout)';

            function createClient(requestTimeout) {
                return function() {
                    var monitor = new Monitor(2500, 3);
                    var options = {};
                    options.requestTimeout = requestTimeout;
                    return new LazyPiratePeer(_testUtil.generateEndpoint(), monitor, options);
                };
            }

            expect(createClient()).to.throw(error);
            expect(createClient(null)).to.throw(error);
            expect(createClient('abc')).to.throw(error);
            expect(createClient(0)).to.throw(error);
            expect(createClient(true)).to.throw(error);
            expect(createClient([])).to.throw(error);
            expect(createClient({})).to.throw(error);
        });

        it('should throw an error if the options object does not define a valid retry count', function() {
            var error = 'Options does not define a valid retry count value (options.retryCount)';

            function createClient(retryCount) {
                return function() {
                    var monitor = new Monitor(2500, 3);
                    var options = { requestTimeout: DEFAULT_REQUEST_TIMEOUT };
                    options.retryCount = retryCount;
                    return new LazyPiratePeer(_testUtil.generateEndpoint(), monitor, options);
                };
            }

            expect(createClient()).to.throw(error);
            expect(createClient(null)).to.throw(error);
            expect(createClient('abc')).to.throw(error);
            expect(createClient(-1)).to.throw(error);
            expect(createClient(true)).to.throw(error);
            expect(createClient([])).to.throw(error);
            expect(createClient({})).to.throw(error);
        });

        it('should create an object that exposes members required by the interface', function() {
            var monitor = new Monitor(2500, 3);
            var client = new LazyPiratePeer(_testUtil.generateEndpoint(), monitor, {
                requestTimeout: DEFAULT_REQUEST_TIMEOUT,
                retryCount: DEFAULT_RETRY_COUNT,
                isServer: false
            });

            expect(client).to.be.an('object');
            expect(client).to.be.an.instanceof(_events.EventEmitter);
            expect(client).to.have.property('initialize').and.to.be.a('function');
            expect(client).to.have.property('send').and.to.be.a('function');
            expect(client).to.have.property('acknowledge').and.to.be.a('function');
            expect(client).to.have.property('dispose').and.to.be.a('function');
        });
    });

    describe('initialize()', function() {

        it('should return a promise when invoked', function() {
            var endpoint = _testUtil.generateEndpoint();
            _client = _createLPClient(endpoint);

            var ret = _client.initialize();

            expect(ret).to.be.an('object');
            expect(ret).to.have.property('then').and.to.be.a('function');
        });

        describe('[CLIENT MODE]', function() {

            it('should reject the promise if an invalid endpoint was specified', function(done) {
                var endpoint = 'bad-endpoint';
                _client = _createLPClient(endpoint);

                expect(_client.initialize()).to.be.rejected.notify(done);
            });

            it('should resolve the promise immediately if a valid endpoint was specified', function(done) {
                var endpoint = _testUtil.generateEndpoint();
                _client = _createLPClient(endpoint);

                expect(_client.initialize()).to.be.fulfilled.notify(done);
            });

            it('should initialize a connection to a peer endpoint when invoked', function(done) {
                var def = _q.defer();
                var endpoint = _testUtil.generateEndpoint();

                _client = _createLPClient(endpoint);
                _peer = _createPeer(endpoint, true);

                _peer.on('accept', function() {
                    // If this event is not triggered, the test will timeout and fail.
                    def.resolve();
                });

                var doTests = function() {
                    return expect(def.promise).to.be.fulfilled;
                };
                
                expect(_client.initialize()).to.be.fulfilled
                    .then(doTests)
                    .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
            });

        });

        describe('[SERVER MODE]', function() {

            it('should reject the promise if an invalid endpoint was specified', function(done) {
                var endpoint = 'bad-endpoint';
                _client = _createLPClient(endpoint, null, { isServer: true });

                expect(_client.initialize()).to.be.rejected.notify(done);
            });

            it('should resolve the promise if a valid endpoint was specified', function(done) {
                var endpoint = _testUtil.generateEndpoint();
                _client = _createLPClient(endpoint, null, { isServer: true });

                expect(_client.initialize()).to.be.fulfilled.notify(done);
            });

            it('should initialize a server socket that can accept connections when invoked', function(done) {
                var def = _q.defer();
                var endpoint = _testUtil.generateEndpoint();

                _client = _createLPClient(endpoint, null, { isServer: true });

                _peer = _createPeer(endpoint, false);
                _peer.on('connect', function() {
                    // If this event is not triggered, the test will timeout and fail.
                    def.resolve();
                });

                var doTests = function() {
                    return expect(def.promise).to.be.fulfilled;
                };
                
                expect(_client.initialize()).to.be.fulfilled
                    .then(doTests)
                    .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
            });

            it('should raise a "ready" event with the client reference after initializing the object', function(done) {
                var handlerSpy = _sinon.spy();
                var endpoint = _testUtil.generateEndpoint();

                _client = _createLPClient(endpoint, null, { isServer: true });
                _client.on(_eventDefinitions.READY, handlerSpy);

                var doTests = function() {
                    expect(handlerSpy).to.have.been.called;
                    expect(handlerSpy).to.have.been.calledWithExactly(_client);
                };

                expect(handlerSpy).to.not.have.been.called;
                expect(_client.initialize()).to.be.fulfilled
                    .then(doTests)
                    .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
            });
        });

    });

    describe('send()', function() {

        it('should throw an error if invoked before the socket has been initialized', function() {
            var error = 'Socket not initialized. Cannot send message';
            var endpoint = _testUtil.generateEndpoint();

            _client = _createLPClient(endpoint);

            expect(function() { _client.send('foo'); }).to.throw(error);
        });

        it('should return a unique id when invoked', function(done) {
            var endpoint = _testUtil.generateEndpoint();

            _client = _createLPClient(endpoint);

            var doTests = function() {
                var id1 = _client.send('foo');
                expect(id1).to.be.a('string');
                expect(id1).to.have.length.of.at.least(1);

                var id2 = _client.send('bar');
                expect(id2).to.be.a('string');
                expect(id2).to.have.length.of.at.least(1);

                expect(id1).to.not.equal(id2);
            };

            expect(_client.initialize()).to.be.fulfilled
                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should send a message over a zero mq socket when invoked', function(done) {
            var def = _q.defer();
            var endpoint = _testUtil.generateEndpoint();
            var receivedFrames = null;
            var messageId = null;
            var clientMessage = 'MESSAGE';

            _peer = _createPeer(endpoint, true);

            _peer.on('message', function(message) {
                receivedFrames = Array.prototype.splice.call(arguments, 0);
                def.resolve();
            });

            _client = _createLPClient(endpoint);
            _client.on(_eventDefinitions.READY, function() {
                messageId = _client.send(clientMessage);
            });

            var doTests = function() {
                expect(receivedFrames).to.be.an('Array');
                expect(receivedFrames).to.have.length(3);
                expect(receivedFrames[0].toString()).to.equal(_messageDefinitions.REQUEST);
                expect(receivedFrames[1].toString()).to.equal(messageId);
                expect(receivedFrames[2].toString()).to.equal(clientMessage);
            };

            expect(_client.initialize()).to.be.fulfilled
                .then(function() { return def.promise; })
                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should allow multiple messages to be dispatched without waiting for acknowledgements/responses from previous messages', function(done) {
            var def = _q.defer();
            var endpoint = _testUtil.generateEndpoint();
            var receivedFrames = [];
            var messageIds = [];
            var clientMessage = 'MESSAGE';
            var messageCount = 3;

            _peer = _createPeer(endpoint, true);
            _peer.on('message', function(message) {
                receivedFrames.push(Array.prototype.splice.call(arguments, 0));
                if(receivedFrames.length == messageCount) {
                    def.resolve();
                }
            });

            _client = _createLPClient(endpoint);
            _client.on(_eventDefinitions.READY, function() {
                for(var index=0; index<messageCount; index++) {
                    messageIds.push(_client.send(clientMessage));
                }
            });

            var doTests = function() {
                expect(receivedFrames).to.have.length(messageCount);
                var index = 0;
                receivedFrames.forEach(function(frames) {
                    expect(frames).to.be.an('Array');
                    expect(frames).to.have.length(3);
                    expect(frames[0].toString()).to.equal(_messageDefinitions.REQUEST);
                    expect(frames[1].toString()).to.equal(messageIds[index]);
                    expect(frames[2].toString()).to.equal(clientMessage);

                    index++;
                });
            };

            expect(_client.initialize()).to.be.fulfilled
                .then(function() { return def.promise; })
                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        describe('[RETRY LOGIC]', function() {
            var POLL_FREQUENCY = 100;
            var REQUEST_TIMEOUT = 200;
            var RETRY_COUNT = 3;

            it('should retry a request if an acknowledgement is not received after a specified duration', function(done){
                var endpoint = _testUtil.generateEndpoint();
                var clientMessage = 'hello';
                var messages = [];
                var testComplete = false;

                function initPeer() {
                    _peer = _createPeer(endpoint, true);
                    _peer.on('message', function(message) {
                        var frames = Array.prototype.splice.call(arguments, 0);
                        messages.push(frames);

                        //Close socket, wait a while, and reopen.
                        _peer.close();
                        _peer = null;
                        
                        setTimeout(function() {
                            if(!testComplete) {
                                initPeer();
                            }
                        }, 100);
                    });
                }

                initPeer();

                _client = _createLPClient(endpoint, new Monitor(POLL_FREQUENCY, -1), {
                    requestTimeout: REQUEST_TIMEOUT,
                    retryCount: RETRY_COUNT
                });

                _client.on(_eventDefinitions.READY, function() {
                    _client.send(clientMessage);
                });

                var doTests = function() {
                    expect(messages).to.have.length.of.at.least(3);
                    var message = null;
                    for(var index=0; index<messages.length; index++) {
                        if(message !== null) {
                            expect(messages[index]).to.deep.equal(message);
                        }
                        message = messages[index];
                    }

                    // Do this to kill the async initialization of the peer socket.
                    // We don't want the method to fire after the test is complete.
                    testComplete = true;
                };

                expect(_client.initialize()).to.be.fulfilled
                    .then(_testUtil.wait(DEFAULT_DELAY + 10))
                    .then(doTests)
                    .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
            });

            it('should abandon retries after a specified number of retries have failed', function(done){
                var endpoint = _testUtil.generateEndpoint();
                var clientMessage = 'hello';
                var messages = [];
                var testComplete = false;

                function initPeer() {
                    _peer = _createPeer(endpoint, true);
                    _peer.on('message', function(message) {
                        var frames = Array.prototype.splice.call(arguments, 0);
                        messages.push(frames);

                        //Close socket, wait a while, and reopen.
                        _peer.close();
                        _peer = null;
                        
                        setTimeout(function() {
                            if(!testComplete) {
                                initPeer();
                            }
                        }, 100);
                    });
                }

                initPeer();

                _client = _createLPClient(endpoint, new Monitor(POLL_FREQUENCY, -1), {
                    requestTimeout: REQUEST_TIMEOUT,
                    retryCount: RETRY_COUNT
                });

                _client.on(_eventDefinitions.READY, function() {
                    _client.send(clientMessage);
                });

                var doTests = function() {
                    expect(messages).to.have.length(RETRY_COUNT);

                    // Do this to kill the async initialization of the peer socket.
                    // We don't want the method to fire after the test is complete.
                    testComplete = true;
                };

                expect(_client.initialize()).to.be.fulfilled
                    .then(_testUtil.wait(DEFAULT_DELAY * 2))
                    .then(doTests)
                    .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
            });

            it('should stop retrying once a valid response has been received from the peer', function(done) {
                var endpoint = _testUtil.generateEndpoint();
                var clientMessage = 'hello';
                var messageCount = 0;

                function initPeer() {
                    _peer = _createPeer(endpoint, true);
                    _peer.on('message', function(message) {
                        var frames = Array.prototype.splice.call(arguments, 0);
                        messageCount++;
                        _peer.send([_messageDefinitions.ACKNOWLEDGE, frames[1]]);
                    });
                }

                initPeer();

                _client = _createLPClient(endpoint, new Monitor(POLL_FREQUENCY, -1), {
                    requestTimeout: REQUEST_TIMEOUT,
                    retryCount: RETRY_COUNT
                });

                _client.on(_eventDefinitions.READY, function() {
                    _client.send(clientMessage);
                });

                var doTests = function() {
                    expect(messageCount).to.equal(1);
                };

                expect(_client.initialize()).to.be.fulfilled
                    .then(_testUtil.wait(DEFAULT_DELAY))
                    .then(doTests)
                    .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
            });
        });
    });

    describe('acknowledge()', function() {

        it('should throw an error if invoked before the socket has been initialized', function() {
            var error = 'Socket not initialized. Cannot send message';
            var endpoint = _testUtil.generateEndpoint();

            _client = _createLPClient(endpoint);

            expect(function() { _client.acknowledge('foo'); }).to.throw(error);
        });

        it('should throw an error if invoked without a valid message id', function() {
            var error = 'Invalid message id specified (arg #1)';

            function invokeMethod(messageId) {
                return function() {
                    var endpoint = _testUtil.generateEndpoint();
                    _client = _createLPClient(endpoint);
                    _client.initialize();
                    _client.acknowledge(messageId);
                };
            }

            expect(invokeMethod()).to.throw(error);
            expect(invokeMethod(null)).to.throw(error);
            expect(invokeMethod('')).to.throw(error);
            expect(invokeMethod(1)).to.throw(error);
            expect(invokeMethod(true)).to.throw(error);
            expect(invokeMethod([])).to.throw(error);
            expect(invokeMethod({})).to.throw(error);
        });

        it('should send an acknowledge message with the specified message id when invoked', function(done) {
            var def = _q.defer();
            var endpoint = _testUtil.generateEndpoint();
            var messageId = 'message-1';
            var message = 'MESSAGE';
            var expectedAck = [ _messageDefinitions.ACKNOWLEDGE, messageId ];
            var ackMessage = null;

            _peer = _createPeer(endpoint);
            _client = _createLPClient(endpoint, null, { isServer: true });

            _peer.on('message', function() {
                ackMessage = Array.prototype.splice.call(arguments, 0);
            });

            _client.on(_eventDefinitions.DATA, function(frames) {
                _client.acknowledge(frames[1].toString());
            });

            var sendMessage = function() {
                _peer.send([ _messageDefinitions.REQUEST, messageId, message ]);
            };

            var doTests = function() {
                expect(ackMessage).to.be.an('Array');
                expect(ackMessage).to.have.length(2);
                expect(ackMessage[0].toString()).to.equal(_messageDefinitions.ACKNOWLEDGE);
                expect(ackMessage[1].toString()).to.equal(messageId);
            };

            expect(_client.initialize()).to.be.fulfilled
                .then(sendMessage)
                .then(_testUtil.wait(DEFAULT_DELAY))
                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });
    });
    
    describe('dispose()', function() {
        it('should close an open socket when invoked.', function(done) {
            var def = _q.defer();
            var endpoint = _testUtil.generateEndpoint();
            var connectionMade = false;

            _peer = _createPeer(endpoint, true);
            _client = _createLPClient(endpoint);

            _peer.on('disconnect', function(message) {
                _testUtil.runDeferred(function() {
                    //Expect that the connection was live prior to the disconnect.
                    expect(connectionMade).to.be.true;
                }, def);
            });

            _client.on(_eventDefinitions.READY, function() {
                connectionMade = true;
                _client.dispose();
            });

            _client.initialize();

            expect(def.promise).to.be.fulfilled.notify(done);
        });
    });

    describe('[EVENTS]', function() {

        it('should emit the "READY" event with the client reference after initializing the object', function(done) {
            var handlerSpy = _sinon.spy();
            var endpoint = _testUtil.generateEndpoint();

            _client = _createLPClient(endpoint);
            _client.on(_eventDefinitions.READY, handlerSpy);

            var doTests = function() {
                expect(handlerSpy).to.have.been.called;
                expect(handlerSpy).to.have.been.calledWithExactly(_client);
            };

            expect(handlerSpy).to.not.have.been.called;
            expect(_client.initialize()).to.be.fulfilled
                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should emit the "DATA" event when data is received from a peer', function(done) {
            var def = _q.defer();
            var endpoint = _testUtil.generateEndpoint();
            var expectedMessage = [_messageDefinitions.REQUEST, 'message-1', 'MESSAGE'];
            var receivedData = null;

            _client = _createLPClient(endpoint, null, { isServer: true });
            _peer = _createPeer(endpoint);
            
            _client.on(_eventDefinitions.DATA, function(frames) {
                receivedData = frames;
                def.resolve();
            });

            var sendData = function() {
                _peer.send(expectedMessage);
            };

            var doTests = function() {
                expect(receivedData).to.be.an('Array');
                expect(receivedData).to.have.length(3);
                for(var index=0; index<receivedData.length; index++) {
                    expect(receivedData[index].toString()).to.deep.equal(expectedMessage[index]);
                }
            };

            expect(_client.initialize()).to.be.fulfilled
                .then(sendData)
                .then(function() { return def.promise; })
                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should not emit the "DATA" event when an acknowledgement is received from a peer', function(done) {
            var def = _q.defer();
            var endpoint = _testUtil.generateEndpoint();
            var ackMessage = [_messageDefinitions.ACKNOWLEDGE, 'message-2'];
            var expectedMessage = [_messageDefinitions.REQUEST, 'message-1', 'MESSAGE'];
            var receivedFrames = [];

            _client = _createLPClient(endpoint, null, { isServer: true });
            _peer = _createPeer(endpoint);
            
            _client.on(_eventDefinitions.DATA, function(frames) {
                receivedFrames.push(frames);
                def.resolve();
            });

            var sendData = function() {
                _peer.send(ackMessage);
                _peer.send(expectedMessage);
            };

            var doTests = function() {
                expect(receivedFrames).to.have.length(1);
                var receivedData = receivedFrames[0];
                expect(receivedData).to.be.an('Array');
                expect(receivedData).to.have.length(3);
                for(var index=0; index<receivedData.length; index++) {
                    expect(receivedData[index].toString()).to.deep.equal(expectedMessage[index]);
                }
            };

            expect(_client.initialize()).to.be.fulfilled
                .then(sendData)
                .then(function() { return def.promise; })
                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });

        it('should emit the "ABANDONED" event when a request is abandoned', function(done){
            var def = _q.defer();
            var endpoint = _testUtil.generateEndpoint();
            var clientMessage = 'hello';
            var expectedMessageId = null;

            _client = _createLPClient(endpoint, new Monitor(100, -1), {
                requestTimeout: 200,
                retryCount: 3
            });

            _client.on(_eventDefinitions.READY, function() {
                expectedMessageId = _client.send(clientMessage);
            });

            _client.on(_eventDefinitions.ABANDONED, function(messageId) {
                def.resolve(messageId);
            });

            var doTests = function(messageId) {
                expect(messageId).to.equal(expectedMessageId);
            };

            expect(_client.initialize()).to.be.fulfilled
                .then(_testUtil.wait(DEFAULT_DELAY * 2))
                .then(function() { return def.promise; })
                .then(doTests)
                .then(_testUtil.getSuccessCallback(done), _testUtil.getFailureCallback(done));
        });
    });
});
