/* jshint node:true, expr:true */
'use strict';

var _zmq = require('zmq');
var _uuid = require('node-uuid');
var _sinon = require('sinon');
var _q = require('q');
var _testUtil = require('./test-util');

var ParanoidPirateQueue = require('../lib/paranoid-pirate-queue');
var _messageDefinitions = require('../lib/message-definitions');

var mod = {
    DEFAULT_DELAY: 100,
    sockets: [],
    context: [],

    setup: function() {
        mod.sockets = [];
        mod.context = [];
    },

    teardown: function() {
        mod.sockets.forEach(function(sock){
            sock.close();
        });
    },

    createReqSocket: function(id) {
        var socket =  _zmq.createSocket('req');
        if(id) {
            socket.identity = id;
        }
        socket.monitor(10);
        mod.sockets.push(socket);
        
        return socket;
    },

    createDealerSocket: function(id) {
        var socket =  _zmq.createSocket('dealer');
        if(id) {
            socket.identity = id;
        }
        socket.monitor(10);
        mod.sockets.push(socket);
        
        return socket;
    },

    captureContext: function(key) {
        return function(data) {
            mod.context[key] = data;
            return data;
        };
    },

    getContext: function(key) {
        return mod.context[key];
    },

    switchContext: function(key) {
        return function() {
            return mod.context[key];
        }
    },

    ensurePPQueueOptions: function(options) {
        options = options || {};
        options.pollFrequency = options.pollFrequency || 1000;
        options.workerTimeout = options.workerTimeout || 3000;
        options.requestTimeout = options.requestTimeout || 3000;

        return options;
    },

    createPPQueue: function(feEndpoint, beEndpoint, queueOptions) {
        feEndpoint = feEndpoint || _testUtil.generateEndpoint();
        beEndpoint = beEndpoint || _testUtil.generateEndpoint();
        queueOptions = mod.ensurePPQueueOptions(queueOptions);
        return new ParanoidPirateQueue(feEndpoint, beEndpoint, queueOptions);
    },

    wait: function(delay) {
        delay = delay || mod.DEFAULT_DELAY;
        return _testUtil.getDelayedRunner(function(data) {
            return data;
        }, delay);
    },

    waitForResolution: function(def) {
        return function() {
            return def.promise;
        };
    },

    initSockets: function(type, count, endpoint, setIds) {
        function getResolver(def, socket) {
            return function() {
               def.resolve(socket); 
            };
        }

        return function() {
            var promises = [];

            for(var index=1; index<=count; index++) {
                var def = _q.defer();
                var id = setIds? _uuid.v4():null;
                var socket = (type === 'req')? mod.createReqSocket(id):
                                                  mod.createDealerSocket(id);

                socket.connect(endpoint);
                socket.on('connect', getResolver(def, socket));

                promises.push(def.promise);
            }
            return _q.all(promises);
        };
    },

    sendMessages: function() {
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
    },

    setupHandlers: function(event, handlers) {
        if(! (handlers instanceof Array)) {
            handlers = [ handlers ];
        }
        return function(sockets) {
            var handlerIndex = 0;
            sockets.forEach(function(socket) {
                var handler = handlers[handlerIndex];
                handlerIndex = (handlerIndex + 1) % handlers.length;

                socket.on(event, handler.bind(socket));
            });

            return sockets;
        };
    },

    createClientRequest: function(service, data) {
        service = service || '';
        data = data || [ 'MESSAGE' ];
        
        return [ _messageDefinitions.REQUEST, service ].concat(data);
    },

    createClientMessages: function(payloads, services, simulateReq) {
        services = services || [ '' ];
        var messages = [];
        var serviceIndex = 0;

        payloads.forEach(function(payload) {
            var service = services[serviceIndex];
            serviceIndex = (serviceIndex + 1) % services.length;

            var request = mod.createClientRequest(service, payload);
            if(simulateReq) {
                // Prefix a '' to each payload. Typically used to simulate traffic
                // from a req socket, when actually using a dealer socket.
                request.unshift('');
            }
            messages.push(request);
        });

        return messages;
    },

    createWorkerMap: function(workerIds) {
        var workerMap = {};
        var handlers = [];
        var workerList = [];
        var count = 0;
        workerIds.forEach(function(workerId) {
            var worker = {
                id: workerId,
                messages: [],
                handler: function() {}
            }

            workerMap[workerId] = worker;
            worker.handler = _sinon.stub(worker, 'handler', function() {
                var frames = Array.prototype.splice.call(arguments, 0);

                worker.messages.push(frames[4].toString());
                if(frames.length < 6 || frames[5].toString() !== 'block') {
                    this.send([_messageDefinitions.RESPONSE, frames[2], frames[3], 'OK']);
                    worker.unblock = function() {};
                } else {
                    var socket = this;
                    worker.unblock = function() {
                        socket.send([_messageDefinitions.RESPONSE, frames[2], frames[3], 'OK']);
                    };
                }
            });

            workerList.push(worker);
            handlers.push(worker.handler);
            count++;
        });

        workerMap.getUnblockAll = function() {
            return function(data) {
                workerMap._workerList.forEach(function(worker) {
                    worker.unblock();
                });
                return data;
            };
        };

        workerMap._workerList = workerList;
        workerMap._handlers = handlers;
        workerMap._count = count;
        return workerMap;
    }

};

module.exports = mod;
