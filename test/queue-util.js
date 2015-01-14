/* jshint node:true, expr:true */
'use strict';

var _zmq = require('zmq');
var _uuid = require('node-uuid');
var _q = require('q');
var _testUtil = require('./test-util');

var ParanoidPirateQueue = require('../lib/paranoid-pirate-queue');

var mod = {
    DEFAULT_DELAY: 10,
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
    }
};

module.exports = mod;
