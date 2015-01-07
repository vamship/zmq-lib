/* jshint node:true, expr:true */
'use strict';

module.exports = {
    MessageDefinitions: require('./message-definitions'),
    EventDefinitions: require('./event-definitions'),
    Monitor: require('./monitor'),
    LazyPirateClient: require('./lazy-pirate-client'),
    SimpleQueue: require('./simple-queue'),
    ParanoidPirateQueue: require('./paranoid-pirate-queue'),
    ParanoidPirateWorker: require('./paranoid-pirate-worker')
};
