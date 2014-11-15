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

var ParanoidPirateQueue = require('../../lib/paranoid-pirate-queue');

