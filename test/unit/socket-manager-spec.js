/* jshint node:true, expr:true */
'use strict';

var _events = require('events');
var _zmq = require('zmq');
var _sinon = require('sinon');
var _sinonChai = require('sinon-chai');
var _chaiAsPromised = require('chai-as-promised');
var _chai = require('chai');
_chai.use(_sinonChai);
_chai.use(_chaiAsPromised);

var expect = _chai.expect;
var _testUtils = require('../test-util');
var SocketManager = require('../../lib/socket-manager');

describe('SocketManager', function() {
    var DEFAULT_ENDPOINT = 'ipc://endpoint';
    var _manager = null;
    var _peer = null;

    function _initRepSocket(callback, endpoint) {
        endpoint = endpoint || DEFAULT_ENDPOINT;

        var socket = _zmq.createSocket('rep');
        socket.monitor(10);
        socket.bind(endpoint, callback);
        return socket;
    }

    function _initReqSocket(endpoint) {
        endpoint = endpoint || DEFAULT_ENDPOINT;

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
            var builder = new SocketManager('req', DEFAULT_ENDPOINT);

            expect(builder).to.have.property('socket').and.to.be.null;
            expect(builder).to.have.property('bindSocket').and.to.be.a('function');
            expect(builder).to.have.property('connectSocket').and.to.be.a('function');
            expect(builder).to.have.property('closeSocket').and.to.be.a('function');
        });
    });
    
    describe('bindSocket()', function() {

        it('should return a promise when invoked', function() {
            _manager = new SocketManager('rep', DEFAULT_ENDPOINT);

            var ret = _manager.bindSocket();
            expect(ret).to.be.an('object');
            expect(ret).to.have.property('then').and.to.be.a('function');
           
        });

        it('should resolve the promise once the socket has been bound successfully', function(){
            _manager = new SocketManager('rep', DEFAULT_ENDPOINT);

            expect(_manager.bindSocket()).to.be.fulfilled;
        });

        it('should reject the promise if the socket cannot be bound correctly', function(){
            _manager = new SocketManager('rep', 'bad-endpoint');

            expect(_manager.bindSocket()).to.be.rejected;
        });

        it('should create and bind a new socket instance when invoked', function(done){
            _manager = new SocketManager('rep', DEFAULT_ENDPOINT);
            expect(_manager.socket).to.be.null;
            _manager.bindSocket().then(function error(err) {

                _testUtils.evaluateExpectations(function() {
                    expect(_manager.socket).to.be.an('object');
                }, done, true);

                _manager.socket.monitor(10);
                _manager.socket.on('accept', function() {
                    done();
                });

                _peer = _initReqSocket(DEFAULT_ENDPOINT);
            });
        });

        it('should throw an error if invoked when the socket is already bound', function() {
            var error = 'Cannot bind/connect. Socket has already been initialized.';

            _manager = new SocketManager('rep', DEFAULT_ENDPOINT);
            _manager.bindSocket();
            expect(function() { _manager.bindSocket(); }).to.throw(error); 
        });

        it('should throw an error if invoked when the socket is already connected', function() {
            var error = 'Cannot bind/connect. Socket has already been initialized.';

            _manager = new SocketManager('rep', DEFAULT_ENDPOINT);
            _manager.connectSocket();
            expect(function() { _manager.bindSocket(); }).to.throw(error); 
        });
    });
    
    describe('connectSocket()', function() {

        it('should create a new socket instance when invoked', function() {
            _manager = new SocketManager('req', DEFAULT_ENDPOINT);
            expect(_manager.socket).to.be.null;
            _manager.connectSocket();
            expect(_manager.socket).to.be.an('object');
        });

        it('should connect to a peer endpoint when invoked', function(done) {
            _manager = new SocketManager('req', DEFAULT_ENDPOINT);
            _peer = _initRepSocket(function(err) {
                if(!err) {
                    _peer.on('accept', function() {
                        done();
                    });

                    _manager.connectSocket();
                    _manager.socket.send('hello');
                }
            }, DEFAULT_ENDPOINT);
        });

        it('should throw an error if the connection fails', function() {
            _manager = new SocketManager('req', 'bad-endpoint');

            expect(function() {
                _manager.connectSocket();
            }).to.throw();
        });

        it('should throw an error if invoked when the socket is already bound', function() {
            var error = 'Cannot bind/connect. Socket has already been initialized.';

            _manager = new SocketManager('req', DEFAULT_ENDPOINT);
            _manager.bindSocket();
            expect(function() { _manager.connectSocket(); }).to.throw(error); 
        });

        it('should throw an error if invoked when the socket is already connected', function() {
            var error = 'Cannot bind/connect. Socket has already been initialized.';

            _manager = new SocketManager('req', DEFAULT_ENDPOINT);
            _manager.bindSocket();
            expect(function() { _manager.connectSocket(); }).to.throw(error); 
        });
    });

    describe('closeSocket()', function() {
        it('should return a promise when invoked', function() {
            _manager = new SocketManager('rep', DEFAULT_ENDPOINT);

            _manager.bindSocket();
            var ret = _manager.closeSocket();
            expect(ret).to.be.an('object');
            expect(ret).to.have.property('then').and.to.be.a('function');
        });

        it('should close the underlying socket when invoked', function(done) {
            _manager = new SocketManager('rep', DEFAULT_ENDPOINT);
            _manager.bindSocket().then(function success() {
                _manager.socket.monitor(10);
                _manager.socket.on('close', function() {
                    // If this is not reached, the test will timeout and fail.
                    done();
                });
                _manager.closeSocket();
                _manager = null;
            });
        });

        it('should set the socket reference to null when invoked', function(done) {
            _manager = new SocketManager('rep', DEFAULT_ENDPOINT);
            _manager.bindSocket().then(function success() {
                _manager.closeSocket().fin(function(){
                    _testUtils.evaluateExpectations(function(){
                        expect(_manager.socket).to.be.null;
                    }, done);
                });
            });
        });

        it('should close the socket gracefully when invoked before binding has completed', function() {
            _manager = new SocketManager('rep', DEFAULT_ENDPOINT);
            _manager.bindSocket();
            _manager.closeSocket();
        })
    });

});
