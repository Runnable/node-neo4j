'use strict'

var assert = require('chai').assert
var EventEmitter = require('events')
var sinon = require('sinon')

var Runna4j = require('../')

describe('Runna4j', function () {
  var graph
  before(function () {
    graph = new Runna4j('localhost:7474')
  })

  describe('constructor', function () {
    it('should accept a host', function () {
      var g = new Runna4j('localhost:1234')
      assert.ok(g)
      // FIXME(bkendall): given cypher-stream, there's no way to check this
    })

    describe('with NEO4J in env', function () {
      beforeEach(function () {
        process.env.NEO4J = 'localhost:5678'
      })
      afterEach(function () {
        process.env.NEO4J = undefined
      })

      it('should accept a host in the env', function () {
        var g = new Runna4j()
        assert.ok(g)
        // FIXME(bkendall): given cypher-stream, there's no way to check this
      })
    })
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

    describe('query errors', function () {
      it('should return an error if query errored', function (done) {
        var error = new Error('foobar')
        graph._query.yieldsAsync(error)
        graph.getNodeCount('Foo', function (err) {
          assert.equal(err, error)
          done()
        })
      })

      it('should return -1 if the count was not returned', function (done) {
        var data = {}
        graph._query.yieldsAsync(null, data)
        graph.getNodeCount('Foo', function (err, count) {
          assert.isNull(err)
          assert.equal(count, -1)
          done()
        })
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

    it('should produce correct queries looking for 1 node with no props (no steps)', function (done) {
      var start = {
        label: 'Foo'
      }
      var steps = []
      var expectedQuery = [
        'MATCH (a:Foo)',
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

    it('should follow steps in with edge properties', function (done) {
      var start = {
        label: 'Foo'
      }
      var steps = [{
        In: {
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
        'MATCH (a:Foo)<-[b:dependsOn]-(c:Foo)',
        'WHERE ' +
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
            bProps: steps[0].In.edge.props,
            cProps: steps[0].In.node.props
          },
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

    it('returns the last node', function (done) {
      var start = { label: 'Foo' }
      var steps = []
      var data = {
        a: 'foo',
        b: 'bar'
      }
      graph._query.yieldsAsync(null, data)
      graph.getNodes(start, steps, function (err, nodes) {
        assert.isNull(err)
        assert.deepEqual(nodes, 'foo')
        done()
      })
    })

    describe('bad steps', function () {
      it('should do nothing with malformed steps', function (done) {
        var start = {
          label: 'Foo',
          props: {
            id: '1234567890asdf',
            someValue: 'sample-node',
            other_value: 1234
          }
        }
        var steps = [{
          foo: 'bar'
        }]
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
    })

    describe('query failures', function () {
      it('should return an error if query errored', function (done) {
        var start = { label: 'Foo' }
        var steps = []
        var error = new Error('foobar')
        graph._query.yieldsAsync(error)
        graph.getNodes(start, steps, function (err, data) {
          assert.equal(err, error)
          done()
        })
      })

      it('should return -1 if the count was not returned', function (done) {
        var start = { label: 'Foo' }
        var steps = []
        var data = {}
        graph._query.yieldsAsync(null, data)
        graph.getNodes(start, steps, function (err, nodes) {
          assert.isNull(err)
          assert.deepEqual(nodes, [])
          done()
        })
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

    describe('query errors', function () {
      var node = {
        label: 'Foo',
        props: {
          id: '1234567890asdf'
        }
      }

      it('should return an error if query errored', function (done) {
        var error = new Error('foobar')
        graph._query.yieldsAsync(error)
        graph.writeNode(node, function (err) {
          assert.equal(err, error)
          done()
        })
      })

      it('should return error if node not created (no data)', function (done) {
        graph._query.yieldsAsync(null, null)
        graph.writeNode(node, function (err) {
          assert.ok(err)
          assert.match(err.message, /node.+not created/i)
          done()
        })
      })

      it('should return error if node not created', function (done) {
        var data = { n: false }
        graph._query.yieldsAsync(null, data)
        graph.writeNode(node, function (err) {
          assert.ok(err)
          assert.match(err.message, /node.+not created/i)
          done()
        })
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

    describe('query failure', function () {
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
      var data
      beforeEach(function () {
        data = {
          a: true,
          r: true,
          b: true
        }
        graph._query.yieldsAsync(null, data)
      })

      it('should return the error', function (done) {
        var error = new Error('foobar')
        graph._query.yieldsAsync(error)
        graph.writeConnection(startNode, connection, endNode, function (err) {
          assert.equal(err, error)
          done()
        })
      })

      describe('missing a', function () {
        beforeEach(function () { data.a = false })

        it('should return an error if no data was returned', function (done) {
          graph.writeConnection(startNode, connection, endNode, function (err) {
            assert.ok(err)
            assert.match(err.message, /relationship.+not created/i)
            done()
          })
        })
      })

      describe('missing r', function () {
        beforeEach(function () { data.r = false })

        it('should return an error if no data was returned', function (done) {
          graph.writeConnection(startNode, connection, endNode, function (err) {
            assert.ok(err)
            assert.match(err.message, /relationship.+not created/i)
            done()
          })
        })
      })

      describe('missing b', function () {
        beforeEach(function () { data.b = false })

        it('should return an error if no data was returned', function (done) {
          graph.writeConnection(startNode, connection, endNode, function (err) {
            assert.ok(err)
            assert.match(err.message, /relationship.+not created/i)
            done()
          })
        })
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

  describe('_query', function () {
    var emitter
    beforeEach(function () {
      emitter = new EventEmitter()
      emitter.commit = function () { emitter.emit('end') }
      sinon.spy(emitter, 'commit')
      emitter.write = sinon.stub()
      sinon.stub(graph.cypher, 'transaction').returns(emitter)
    })
    afterEach(function () {
      graph.cypher.transaction.restore()
    })

    it('should get the transaction from cypher', function (done) {
      graph._query('query', 'params', function (err) {
        assert.isNull(err)
        sinon.assert.calledOnce(graph.cypher.transaction)
        done()
      })
    })

    it('should write the passed query and params', function (done) {
      graph._query('query', 'params', function (err) {
        assert.isNull(err)
        sinon.assert.calledOnce(emitter.write)
        sinon.assert.calledWithExactly(
          emitter.write,
          {
            statement: 'query',
            parameters: 'params'
          }
        )
        done()
      })
    })

    it('should ignore empty data events', function (done) {
      emitter.commit = function () {
        emitter.emit('data')
        emitter.emit('end')
      }
      graph._query({}, {}, function (err, result) {
        assert.isNull(err)
        assert.deepEqual(result, {})
        done()
      })
    })

    it('should return the data from the data events', function (done) {
      emitter.commit = function () {
        emitter.emit('data', { 'foo': 'bar' })
        emitter.emit('end')
      }
      graph._query({}, {}, function (err, result) {
        assert.isNull(err)
        assert.deepEqual(result, { foo: [ 'bar' ] })
        done()
      })
    })

    it('should collect data events', function (done) {
      emitter.commit = function () {
        emitter.emit('data', { 'foo': 'bar' })
        emitter.emit('data', { 'foo': 'baz' })
        emitter.emit('end')
      }
      graph._query({}, {}, function (err, result) {
        assert.isNull(err)
        assert.deepEqual(result, { foo: [ 'bar', 'baz' ] })
        done()
      })
    })

    it('should commit the transaction', function (done) {
      graph._query({}, {}, function (err) {
        assert.isNull(err)
        sinon.assert.calledOnce(emitter.commit)
        done()
      })
    })

    describe('errors', function () {
      it('should return errors', function (done) {
        var error = new Error('foobar')
        emitter.commit = function () {
          emitter.emit('error', error)
          emitter.emit('end')
        }
        graph._query({}, {}, function (err) {
          assert.equal(err, error)
          done()
        })
      })
    })
  })
})
