/* jshint node:true, expr:true */
'use strict';
var expect = require('chai').expect;
var _index = require('../../lib/index');


describe('index', function() {
    it('should implement methods required by the interface', function() {
        expect(_index).to.have.property('Monitor').and.to.be.a('function');
        expect(_index).to.have.property('LazyPirateClient').and.to.be.a('function');
    });
});
