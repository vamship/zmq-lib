/* jshint node:true, expr:true */
'use strict';

var expect = require('chai').expect;
var _index = require('../../lib/index');

describe('index', function() {
    it('should implement methods required by the interface', function() {
        expect(_index).to.have.property('Monitor').and.to.be.a('function');
        expect(_index).to.have.property('LazyPirateClient').and.to.be.a('function');
        expect(_index).to.have.property('LazyPiratePair').and.to.be.a('function');
        expect(_index).to.have.property('SimpleQueue').and.to.be.a('function');
        expect(_index).to.have.property('ParanoidPirateQueue').and.to.be.a('function');
        expect(_index).to.have.property('ParanoidPirateWorker').and.to.be.a('function');
        expect(_index).to.have.property('MessageDefinitions').and.to.be.an('object');
        expect(_index).to.have.property('EventDefinitions').and.to.be.an('object');
    });
});
