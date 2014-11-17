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
var Monitor = require('../../lib/monitor');
var ParanoidPirateWorker = require('../../lib/paranoid-pirate-worker');
var MessageDefinitions = require('../../lib/message-definitions');

describe('ParanoidPirateWorker', function(){
    var DEFAULT_RETRY_FREQ = 2500;
    var DEFAULT_RETRY_COUNT = 3;
    var _queue;
    var _worker;

    function _createPPWorker(endpoint, monitor) {
        monitor = monitor || new Monitor(DEFAULT_RETRY_FREQ, DEFAULT_RETRY_COUNT);
        var worker = new ParanoidPirateWorker(endpoint, monitor);

        return worker;
    }

    function _createQueue(endpoint) {
        var socket = _zmq.createSocket('router');
        socket.monitor(10);
        socket.bind(endpoint);

        return socket;
    }

    afterEach(function() {
        // Clean up resources. This will happen even if tests fail.
        if(_queue){
            _queue.close();
        }
        if(_worker){
            _worker.dispose();
        }
        _worker = null;
        _queue = null;
    });


    describe('ctor()', function() {
        it('should throw an error if a valid endpoint value is not specified', function() {
            var error = 'Invalid endpoint specified (arg #1)';

            function createClient(endpoint) {
                return function() {
                    return new ParanoidPirateWorker(endpoint);
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
                    return new ParanoidPirateWorker(_testUtils.generateEndpoint(), monitor);
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
            var client = new ParanoidPirateWorker(_testUtils.generateEndpoint(), monitor);

            expect(client).to.be.an('object');
            expect(client).to.be.an.instanceof(_events.EventEmitter);
            expect(client).to.have.property('initialize').and.to.be.a('function');
            expect(client).to.have.property('send').and.to.be.a('function');
            expect(client).to.have.property('dispose').and.to.be.a('function');
            expect(client).to.have.property('isReady').and.to.be.a('function');
        });

        it('should set property values to defaults', function() {
            var monitor = new Monitor(2500, 3);
            var client = new ParanoidPirateWorker(_testUtils.generateEndpoint(), monitor);

            expect(client.isReady()).to.be.false;
        });
    });

    describe('initialize()', function() {
        it('should initialize a connection to a peer endpoint when invoked', function(done) {
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();

            _worker = _createPPWorker(endpoint);
            _queue = _createQueue(endpoint);

            _queue.on('accept', function() {
                // If this event is not triggered, the test will timeout and fail.
                def.resolve();
            });
            expect(def.promise).to.be.fulfilled.notify(done);
            
            _worker.initialize();
        });

        it('should send a READY message to the queue when invoked', function(done){
            var def = _q.defer();
            var endpoint = _testUtils.generateEndpoint();

            _worker = _createPPWorker(endpoint);
            _queue = _createQueue(endpoint);

            _queue.on('message', function() {
                var frames = Array.prototype.splice.call(arguments, 0);

                _testUtils.runDeferred(function() {
                    expect(frames).to.have.length(3);
                    expect(frames[1].toString()).to.equal('');
                    expect(frames[2].toString()).to.equal(MessageDefinitions.READY);
                }, def);
            });

            expect(def.promise).to.be.fulfilled.notify(done);
            
            _worker.initialize();

        });

        it('should raise a "ready" event with null args after initializing the object', function() {
            var handlerSpy = _sinon.spy();
            var endpoint = _testUtils.generateEndpoint();

            _worker = _createPPWorker(endpoint);
            _worker.on('ready', handlerSpy);

            expect(handlerSpy).to.not.have.been.called;
            _worker.initialize();
            expect(handlerSpy).to.have.been.called;
            expect(handlerSpy).to.have.been.calledWithExactly(null);
        });
    });
});
