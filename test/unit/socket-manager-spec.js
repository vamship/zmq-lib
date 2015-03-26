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
var SocketManager = require('../../lib/socket-manager');

describe('SocketManager', function() {
    var _manager = null;
    var _peer = null;

    function _initRepSocket(callback, endpoint) {
        var socket = _zmq.createSocket('rep');
        socket.monitor(10);
        socket.bind(endpoint, callback);
        return socket;
    }

    function _initReqSocket(endpoint) {
        var socket = _zmq.createSocket('req');
        socket.monitor(10);
        socket.connect(endpoint);
        return socket;
    }

    afterEach(function(){
        if(_manager) {
            _manager.closeSocket();
        }
        if(_peer) {
            _peer.close();
        }
        _manager = null;
        _peer = null;
    });

    describe('ctor()', function() {
        it('should throw an error if a valid socket type is not specified', function() {
            var error = 'Invalid socket type specified (arg #1)';

            function createBuilder(type) {
                return function() {
                    return new SocketManager(type);
                }
            }

            expect(createBuilder()).to.throw(error);
            expect(createBuilder(null)).to.throw(error);
            expect(createBuilder('')).to.throw(error);
            expect(createBuilder(1)).to.throw(error);
            expect(createBuilder(true)).to.throw(error);
            expect(createBuilder({})).to.throw(error);
            expect(createBuilder([])).to.throw(error);
        });

        it('should throw an error if a valid endpoint is not specified', function() {
            var error = 'Invalid endpoint specified (arg #2)';

            function createBuilder(type) {
                return function() {
                    return new SocketManager('req', type);
                }
            }

            expect(createBuilder()).to.throw(error);
            expect(createBuilder(null)).to.throw(error);
            expect(createBuilder('')).to.throw(error);
            expect(createBuilder(1)).to.throw(error);
            expect(createBuilder(true)).to.throw(error);
            expect(createBuilder({})).to.throw(error);
            expect(createBuilder([])).to.throw(error);
        });

        it('should create an object that exposes members required by the interface', function() {
            var endpoint = _testUtils.generateEndpoint();
            var builder = new SocketManager('req', endpoint);

            expect(builder).to.have.property('socket').and.to.be.null;
            expect(builder).to.have.property('bindSocket').and.to.be.a('function');
            expect(builder).to.have.property('connectSocket').and.to.be.a('function');
            expect(builder).to.have.property('closeSocket').and.to.be.a('function');
        });
    });
    
    describe('bindSocket()', function() {

        it('should return a promise when invoked', function() {
            var endpoint = _testUtils.generateEndpoint();
            _manager = new SocketManager('rep', endpoint);

            var ret = _manager.bindSocket();
            expect(ret).to.be.an('object');
            expect(ret).to.have.property('then').and.to.be.a('function');
           
        });

        it('should resolve the promise once the socket has been bound successfully', function(done){
            var endpoint = _testUtils.generateEndpoint();
            _manager = new SocketManager('rep', endpoint);

            expect(_manager.bindSocket()).to.be.fulfilled.notify(done);
        });

        it('should reject the promise if the socket cannot be bound correctly', function(done){
            _manager = new SocketManager('rep', 'bad-endpoint');

            expect(_manager.bindSocket()).to.be.rejected.notify(done);
        });

        it('should create and bind a new socket instance when invoked', function(done){
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();

            _manager = new SocketManager('rep', endpoint);
            expect(_manager.socket).to.be.null;
            _manager.bindSocket().then(function error(err) {

                _testUtils.runDeferred(function() {
                    expect(_manager.socket).to.be.an('object');
                }, def);

                _manager.socket.monitor(10);
                _manager.socket.on('accept', function() {
                    // Reject/resolve based on whether the tests passed or failed.
                    expect(def.promise).to.be.fulfilled.then(function() { done(); },
                                                             function(err) { done(err); });
                });

                _peer = _initReqSocket(endpoint);
            });
        });

        it('should throw an error if invoked when the socket is already bound', function() {
            var endpoint = _testUtils.generateEndpoint();
            var error = 'Cannot bind/connect. Socket has already been initialized.';

            _manager = new SocketManager('rep', endpoint);
            _manager.bindSocket();
            expect(function() { _manager.bindSocket(); }).to.throw(error); 
        });

        it('should throw an error if invoked when the socket is already connected', function() {
            var endpoint = _testUtils.generateEndpoint();
            var error = 'Cannot bind/connect. Socket has already been initialized.';

            _manager = new SocketManager('rep', endpoint);
            _manager.connectSocket();
            expect(function() { _manager.bindSocket(); }).to.throw(error); 
        });
    });
    
    describe('connectSocket()', function() {

        it('should create a new socket instance when invoked', function() {
            var endpoint = _testUtils.generateEndpoint();
            _manager = new SocketManager('req', endpoint);
            expect(_manager.socket).to.be.null;
            _manager.connectSocket();
            expect(_manager.socket).to.be.an('object');
        });

        it('should connect to a peer endpoint when invoked', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();
            _manager = new SocketManager('req', endpoint);
            _peer = _initRepSocket(function(err) {
                if(!err) {
                    _peer.on('accept', function() {
                        def.resolve();
                    });

                    _manager.connectSocket();
                    _manager.socket.send('hello');
                }
            }, endpoint);

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should throw an error if the connection fails', function() {
            _manager = new SocketManager('req', 'bad-endpoint');

            expect(function() {
                _manager.connectSocket();
            }).to.throw();
        });

        it('should throw an error if invoked when the socket is already bound', function() {
            var endpoint = _testUtils.generateEndpoint();
            var error = 'Cannot bind/connect. Socket has already been initialized.';

            _manager = new SocketManager('req', endpoint);
            _manager.bindSocket();
            expect(function() { _manager.connectSocket(); }).to.throw(error); 
        });

        it('should throw an error if invoked when the socket is already connected', function() {
            var endpoint = _testUtils.generateEndpoint();
            var error = 'Cannot bind/connect. Socket has already been initialized.';

            _manager = new SocketManager('req', endpoint);
            _manager.bindSocket();
            expect(function() { _manager.connectSocket(); }).to.throw(error); 
        });
    });

    describe('closeSocket()', function() {
        it('should return a promise when invoked', function() {
            var endpoint = _testUtils.generateEndpoint();
            _manager = new SocketManager('rep', endpoint);

            _manager.bindSocket();
            var ret = _manager.closeSocket();
            expect(ret).to.be.an('object');
            expect(ret).to.have.property('then').and.to.be.a('function');
        });

        it('should close the underlying socket when invoked', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();
            _manager = new SocketManager('rep', endpoint);
            _manager.bindSocket().then(function success() {
                _manager.socket.monitor(10);
                _manager.socket.on('close', function() {
                    // If this is not reached, the test will timeout and fail.
                    def.resolve();
                });
                _manager.closeSocket();
                _manager = null;
            });

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should resolve the promise once the socket has been closed', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();
            _manager = new SocketManager('rep', endpoint);
            _manager.bindSocket().then(function success() {
                var closePromise = null;
                _manager.socket.monitor(10);
                _manager.socket.on('close', function() {
                    closePromise.then(function() {
                        // If this is not reached, the test will timeout and fail.
                        def.resolve();
                    });
                });
                closePromise = _manager.closeSocket();
                _manager = null;
            });

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should set the socket reference to null when invoked', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();
            _manager = new SocketManager('rep', endpoint);
            _manager.bindSocket().then(function success() {
                _manager.closeSocket().fin(function(){
                    _testUtils.runDeferred(function(){
                        expect(_manager.socket).to.be.null;
                    }, def);
                });
            });

            expect(def.promise).to.be.fulfilled.notify(done);
        });

        it('should close the socket gracefully when invoked before binding has completed', function() {
            var endpoint = _testUtils.generateEndpoint();
            _manager = new SocketManager('rep', endpoint);
            _manager.bindSocket();
            _manager.closeSocket();
        })

        it('should reject the promise if there are errors closing the socket', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();
            _manager = new SocketManager('rep', endpoint);
            _manager.socket = {};
            _manager.closeSocket().then(function() {
                def.resolve();
            }, function(err) {
                def.reject(err);
            });

            expect(def.promise).to.be.rejectedWith('undefined is not a function').notify(done);
        });

    });

});
