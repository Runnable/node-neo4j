'use strict'

var assert = require('chai').assert
var sinon = require('sinon')

var Runna4j = require('../')

describe('Runna4j', function () {
  var graph
  before(function () {
    graph = new Runna4j('localhost:7474')
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
    beforeEach(function () {
      sinon.stub(graph, '_query').yieldsAsync(null, null)
    })
    afterEach(function () {
      graph._query.restore()
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

  describe('writeNodes', function () {
    beforeEach(function () {
      sinon.stub(graph, '_writeUniqueNode').yieldsAsync(null)
    })
    afterEach(function () {
      graph._writeUniqueNode.restore()
    })

    it('should just return with an empty list', function (done) {
      var nodes = []
      graph.writeNodes(nodes, function (err) {
        assert.isNull(err)
        sinon.assert.notCalled(graph._writeUniqueNode)
        done()
      })
    })

    it('should simply call into write connection for a list', function (done) {
      var nodes = [{
        label: 'Foo',
        props: {
          id: '1234567890asdf'
        }
      }, {
        label: 'Bar',
        props: {
          id: 'DeadBeef'
        }
      }]
      graph.writeNodes(nodes, function (err) {
        assert.isNull(err)
        sinon.assert.calledTwice(graph._writeUniqueNode)
        sinon.assert.calledWithExactly(
          graph._writeUniqueNode,
          nodes[0],
          sinon.match.func
        )
        sinon.assert.calledWithExactly(
          graph._writeUniqueNode,
          nodes[1],
          sinon.match.func
        )
        done()
      })
    })
  })

  describe('writeNode', function () {
    beforeEach(function () {
      // fake the node being created
      sinon.stub(graph, '_query').yieldsAsync(null, { n: true })
    })
    afterEach(function () {
      graph._query.restore()
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
    beforeEach(function () {
      // fake the node being created
      sinon.stub(graph, '_query').yieldsAsync(null, { n: true })
    })
    afterEach(function () {
      graph._query.restore()
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

  describe('writeConnections', function () {
    beforeEach(function () {
      sinon.stub(graph, '_createUniqueRelationship').yieldsAsync(null)
    })
    afterEach(function () {
      graph._createUniqueRelationship.restore()
    })

    it('should just return with an empty list', function (done) {
      var connections = []
      graph.writeConnections(connections, function (err) {
        assert.isNull(err)
        sinon.assert.notCalled(graph._createUniqueRelationship)
        done()
      })
    })

    it('should simply call into write connection for a list', function (done) {
      var connections = [
        ['Foo', 'bar', 'Baz'],
        ['Abc', 'def', 'Ghi']
      ]
      graph.writeConnections(connections, function (err) {
        assert.isNull(err)
        sinon.assert.calledTwice(graph._createUniqueRelationship)
        sinon.assert.calledWithExactly(
          graph._createUniqueRelationship,
          'Foo',
          'bar',
          'Baz',
          sinon.match.func
        )
        sinon.assert.calledWithExactly(
          graph._createUniqueRelationship,
          'Abc',
          'def',
          'Ghi',
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

  describe('deleteConnections', function () {
    beforeEach(function () {
      sinon.stub(graph, '_deleteConnection').yieldsAsync(null)
    })
    afterEach(function () {
      graph._deleteConnection.restore()
    })

    it('should just return with an empty list', function (done) {
      var connections = []
      graph.deleteConnections(connections, function (err) {
        assert.isNull(err)
        sinon.assert.notCalled(graph._deleteConnection)
        done()
      })
    })

    it('should simply call into delete connection for a list', function (done) {
      var connections = [{
        subject: 'Foo',
        predicate: 'bar',
        object: 'Baz'
      }, {
        subject: 'Abc',
        predicate: 'def',
        object: 'Ghi'
      }]
      graph.deleteConnections(connections, function (err) {
        assert.isNull(err)
        sinon.assert.calledTwice(graph._deleteConnection)
        sinon.assert.calledWithExactly(
          graph._deleteConnection,
          { id: 'Foo' },
          'bar',
          { id: 'Baz' },
          sinon.match.func
        )
        sinon.assert.calledWithExactly(
          graph._deleteConnection,
          { id: 'Abc' },
          'def',
          { id: 'Ghi' },
          sinon.match.func
        )
        done()
      })
    })
  })

  describe('deleteConnection', function () {
    beforeEach(function () {
      sinon.stub(graph, '_query').yieldsAsync(null, { a: true, b: true, r: true })
    })
    afterEach(function () {
      graph._query.restore()
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
