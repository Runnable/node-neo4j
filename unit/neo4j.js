'use strict'

var assert = require('chai').assert
var sinon = require('sinon')

var Runna4j = require('../')

describe('Runna4j', function () {
  var graph
  before(function (done) {
    graph = new Runna4j('localhost:7474')
    done()
  })

  describe('getNodeCount', function () {
    beforeEach(function () {
      sinon.stub(graph, '_query')
    })
    afterEach(function () {
      graph._query.restore()
    })

    it('should be able make a query to get the count of nodes', function (done) {
      graph._query.yieldsAsync(null, { 'count(*)': [1] })
      var expectedQuery = 'MATCH (n:Foo) RETURN count(*)'
      graph.getNodeCount('Foo', function (err, data) {
        assert.isNull(err)
        assert.equal(data, 1) // because that's what we set the stub to
        sinon.assert.calledOnce(graph._query)
        sinon.assert.calledWithExactly(
          graph._query,
          expectedQuery,
          {},
          sinon.match.func
        )
        done()
      })
    })
  })

  describe('getNodes', function () {
    beforeEach(function (done) {
      sinon.stub(graph, '_query').yieldsAsync(null, null)
      done()
    })
    afterEach(function (done) {
      graph._query.restore()
      done()
    })

    it('should produce correct queries looking for 1 node (no steps)', function (done) {
      var start = {
        label: 'Foo',
        props: {
          id: '1234567890asdf',
          someValue: 'sample-node',
          other_value: 1234
        }
      }
      var steps = []
      var expectedQuery = [
        'MATCH (a:Foo)',
        'WHERE ' +
        'a.id={props}.id AND ' +
        'a.someValue={props}.someValue AND ' +
        'a.other_value={props}.other_value',
        'RETURN a'
      ].join('\n')
      graph.getNodes(start, steps, function (err, data) {
        assert.isNull(err)
        assert.isNull(data)
        sinon.assert.calledOnce(graph._query)
        sinon.assert.calledWithExactly(
          graph._query,
          expectedQuery,
          { props: start.props },
          sinon.match.func
        )
        done()
      })
    })

    it('should produce correct queries looking for node via out steps', function (done) {
      var start = {
        label: 'Foo',
        props: {
          id: '1234567890asdf',
          someValue: 'sample-node',
          other_value: 1234
        }
      }
      var steps = [{
        Out: {
          edge: { label: 'dependsOn' },
          node: { label: 'Foo' }
        }
      }]
      var expectedQuery = [
        'MATCH (a:Foo)-[b:dependsOn]->(c:Foo)',
        'WHERE ' +
        'a.id={props}.id AND ' +
        'a.someValue={props}.someValue AND ' +
        'a.other_value={props}.other_value',
        'RETURN a,b,c'
      ].join('\n')
      graph.getNodes(start, steps, function (err, data) {
        assert.isNull(err)
        assert.isNull(data)
        sinon.assert.calledOnce(graph._query)
        sinon.assert.calledWithExactly(
          graph._query,
          expectedQuery,
          { props: start.props },
          sinon.match.func
        )
        done()
      })
    })

    it('should produce correct queries looking for node via out multiple steps', function (done) {
      var start = {
        label: 'Foo',
        props: {
          id: '1234567890asdf',
          someValue: 'sample-node',
          owner_value: 1234
        }
      }
      var steps = [{
        Out: {
          edge: { label: 'dependsOn' },
          node: { label: 'Foo' }
        }
      }, {
        Out: {
          edge: { label: 'hasThing' },
          node: {
            label: 'Bar',
            props: { id: 'deadbeef' }
          }
        }
      }]
      var expectedQuery = [
        'MATCH (a:Foo)-[b:dependsOn]->(c:Foo)-[d:hasThing]->(e:Bar)',
        'WHERE ' +
        'a.id={props}.id AND ' +
        'a.someValue={props}.someValue AND ' +
        'a.owner_value={props}.owner_value AND ' +
        'e.id={eProps}.id',
        'RETURN a,b,c,d,e'
      ].join('\n')
      graph.getNodes(start, steps, function (err, data) {
        assert.isNull(err)
        assert.isNull(data)
        sinon.assert.calledOnce(graph._query)
        sinon.assert.calledWithExactly(
          graph._query,
          expectedQuery,
          {
            props: start.props,
            eProps: steps[1].Out.node.props
          },
          sinon.match.func
        )
        done()
      })
    })

    it('should produce correct queries looking for node via in steps', function (done) {
      var start = {
        label: 'Foo',
        props: {
          id: '1234567890asdf',
          someValue: 'sample-node',
          other_value: 1234
        }
      }
      var steps = [{
        In: {
          edge: { label: 'dependsOn' },
          node: { label: 'Foo' }
        }
      }]
      var expectedQuery = [
        'MATCH (a:Foo)<-[b:dependsOn]-(c:Foo)',
        'WHERE ' +
        'a.id={props}.id AND ' +
        'a.someValue={props}.someValue AND ' +
        'a.other_value={props}.other_value',
        'RETURN a,b,c'
      ].join('\n')
      graph.getNodes(start, steps, function (err, data) {
        assert.isNull(err)
        assert.isNull(data)
        sinon.assert.calledOnce(graph._query)
        sinon.assert.calledWithExactly(
          graph._query,
          expectedQuery,
          { props: start.props },
          sinon.match.func
        )
        done()
      })
    })

    it('should follow steps with edge properties', function (done) {
      var start = {
        label: 'Foo',
        props: {
          id: '1234567890asdf',
          someValue: 'sample-node',
          other_value: 1234
        }
      }
      var steps = [{
        Out: {
          edge: {
            label: 'dependsOn',
            props: { hostname: 'somehostname' }
          },
          node: {
            label: 'Foo',
            props: { someValue: 'somename' }
          }
        }
      }]
      var expectedQuery = [
        'MATCH (a:Foo)-[b:dependsOn]->(c:Foo)',
        'WHERE ' +
        'a.id={props}.id AND ' +
        'a.someValue={props}.someValue AND ' +
        'a.other_value={props}.other_value AND ' +
        'b.hostname={bProps}.hostname AND ' +
        'c.someValue={cProps}.someValue',
        'RETURN a,b,c'
      ].join('\n')
      graph.getNodes(start, steps, function (err, data) {
        assert.isNull(err)
        assert.isNull(data)
        sinon.assert.calledOnce(graph._query)
        sinon.assert.calledWithExactly(
          graph._query,
          expectedQuery,
          {
            props: start.props,
            bProps: steps[0].Out.edge.props,
            cProps: steps[0].Out.node.props
          },
          sinon.match.func
        )
        done()
      })
    })

    it('should follow steps with node properties', function (done) {
      var start = {
        label: 'Foo',
        props: {
          id: '1234567890asdf',
          someValue: 'sample-node',
          other_value: 1234
        }
      }
      var steps = [{
        Out: {
          edge: { label: 'dependsOn' },
          node: {
            label: 'Foo',
            props: { someValue: 'some-name' }
          }
        }
      }]
      var expectedQuery = [
        'MATCH (a:Foo)-[b:dependsOn]->(c:Foo)',
        'WHERE ' +
        'a.id={props}.id AND ' +
        'a.someValue={props}.someValue AND ' +
        'a.other_value={props}.other_value AND ' +
        'c.someValue={cProps}.someValue',
        'RETURN a,b,c'
      ].join('\n')
      graph.getNodes(start, steps, function (err, data) {
        assert.isNull(err)
        assert.isNull(data)
        sinon.assert.calledOnce(graph._query)
        sinon.assert.calledWithExactly(
          graph._query,
          expectedQuery,
          {
            props: start.props,
            cProps: steps[0].Out.node.props
          },
          sinon.match.func
        )
        done()
      })
    })
  })

  describe('writeNode', function () {
    beforeEach(function (done) {
      // fake the node being created
      sinon.stub(graph, '_query').yieldsAsync(null, { n: true })
      done()
    })
    afterEach(function (done) {
      graph._query.restore()
      done()
    })

    it('should make a query to write in a unique node', function (done) {
      var node = {
        label: 'Foo',
        props: {
          id: '1234567890asdf',
          someValue: 'sample-node',
          other_value: 1234
        }
      }
      var expectedQuery = [
        'MERGE (n:Foo {id: {props}.id})',
        'ON CREATE SET n.someValue = {props}.someValue, ' +
        'n.other_value = {props}.other_value',
        'ON MATCH SET n.someValue = {props}.someValue, ' +
        'n.other_value = {props}.other_value',
        'RETURN n'
      ].join('\n')
      graph.writeNode(node, function (err) {
        assert.isNull(err)
        sinon.assert.calledOnce(graph._query)
        sinon.assert.calledWithExactly(
          graph._query,
          expectedQuery,
          { props: node.props },
          sinon.match.func
        )
        done()
      })
    })

    it('should make a query to write in a unique node with no props except id', function (done) {
      var node = {
        label: 'Foo',
        props: {
          id: '1234567890asdf'
        }
      }
      var expectedQuery = [
        'MERGE (n:Foo {id: {props}.id})',
        'RETURN n'
      ].join('\n')
      graph.writeNode(node, function (err) {
        assert.isNull(err)
        sinon.assert.calledOnce(graph._query)
        sinon.assert.calledWithExactly(
          graph._query,
          expectedQuery,
          { props: node.props },
          sinon.match.func
        )
        done()
      })
    })
  })

  describe('deleteNodeAndConnections', function () {
    beforeEach(function (done) {
      // fake the node being created
      sinon.stub(graph, '_query').yieldsAsync(null, { n: true })
      done()
    })
    afterEach(function (done) {
      graph._query.restore()
      done()
    })

    it('should make a query to delete the node and all connections', function (done) {
      var node = {
        label: 'Foo',
        props: {
          id: '1234567890asdf'
        }
      }
      var expectedQuery = [
        'MATCH (n:Foo {id: {props}.id})',
        'OPTIONAL MATCH (n)-[r]-()',
        'DELETE n,r'
      ].join('\n')
      graph.deleteNodeAndConnections(node, function (err) {
        assert.isNull(err)
        sinon.assert.calledOnce(graph._query)
        sinon.assert.calledWithExactly(
          graph._query,
          expectedQuery,
          { props: node.props },
          sinon.match.func
        )
        done()
      })
    })
  })

  describe('writeConnection', function () {
    beforeEach(function (done) {
      sinon.stub(graph, '_query').yieldsAsync(null, { a: true, b: true, r: true })
      done()
    })
    afterEach(function (done) {
      graph._query.restore()
      done()
    })

    it('should make a query to write a connection', function (done) {
      var startNode = {
        label: 'Foo',
        props: {
          id: '1234567890asdf'
        }
      }
      var connection = {
        label: 'dependsOn'
      }
      var endNode = {
        label: 'Foo',
        props: {
          id: 'fdsa0987654321'
        }
      }
      var expectedQuery = [
        'MATCH (a:Foo {id: {startProps}.id}),' +
        '(b:Foo {id: {endProps}.id})',
        'MERGE (a)-[r:dependsOn]->(b)',
        'RETURN a,r,b'
      ].join('\n')
      graph.writeConnection(startNode, connection, endNode, function (err) {
        assert.isNull(err)
        sinon.assert.calledOnce(graph._query)
        sinon.assert.calledWithExactly(
          graph._query,
          expectedQuery,
          {
            startProps: startNode.props,
            endProps: endNode.props
          },
          sinon.match.func
        )
        done()
      })
    })

    it('should make a query to write a connection with props', function (done) {
      var startNode = {
        label: 'Foo',
        props: {
          id: '1234567890asdf'
        }
      }
      var connection = {
        label: 'dependsOn',
        props: {
          since: 'forever'
        }
      }
      var endNode = {
        label: 'Foo',
        props: {
          id: 'fdsa0987654321'
        }
      }
      var expectedQuery = [
        'MATCH (a:Foo {id: {startProps}.id}),' +
        '(b:Foo {id: {endProps}.id})',
        'MERGE (a)-[r:dependsOn]->(b)',
        "ON CREATE SET r.since='forever'",
        "ON MATCH SET r.since='forever'",
        'RETURN a,r,b'
      ].join('\n')
      graph.writeConnection(startNode, connection, endNode, function (err) {
        assert.isNull(err)
        sinon.assert.calledOnce(graph._query)
        sinon.assert.calledWithExactly(
          graph._query,
          expectedQuery,
          {
            startProps: startNode.props,
            endProps: endNode.props
          },
          sinon.match.func
        )
        done()
      })
    })
  })

  describe('deleteConnection', function () {
    beforeEach(function (done) {
      sinon.stub(graph, '_query').yieldsAsync(null, { a: true, b: true, r: true })
      done()
    })
    afterEach(function (done) {
      graph._query.restore()
      done()
    })

    it('should make a query to remove a connection', function (done) {
      var startNode = {
        label: 'Foo',
        props: {
          id: '1234567890asdf'
        }
      }
      var connectionLabel = 'dependsOn'
      var endNode = {
        label: 'Foo',
        props: {
          id: 'fdsa0987654321'
        }
      }
      var expectedQuery = [
        'MATCH (a:Foo {id: {startProps}.id})-[r:dependsOn]->(b:Foo {id: {endProps}.id})',
        'DELETE r'
      ].join('\n')
      graph.deleteConnection(startNode, connectionLabel, endNode, function (err) {
        assert.isNull(err)
        sinon.assert.calledOnce(graph._query)
        sinon.assert.calledWithExactly(
          graph._query,
          expectedQuery,
          {
            startProps: startNode.props,
            endProps: endNode.props
          },
          sinon.match.func
        )
        done()
      })
    })
  })
})
