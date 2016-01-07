'use strict'

var async = require('async')
var backlight = require('blacklight')
var cypher = require('cypher-stream')
var debug = require('debug')('runna4j')
var keypather = require('keypather')()
var last = require('101/last')

module.exports = Neo4j

function Neo4j (host) {
  this.cypher = cypher(host || process.env.NEO4J)
}

Neo4j.prototype.getNodeCount = function (nodeLabel, cb) {
  debug('Neo4j#getNodeCount', nodeLabel)
  var query = 'MATCH (n:' + nodeLabel + ') RETURN count(*)'
  this._query(query, {}, function (err, data) {
    if (err) {
      cb(err)
    } else if (!data || !data['count(*)']) {
      debug('Neo4j#getNodeCount', nodeLabel, -1)
      cb(null, -1)
    } else {
      var count = data['count(*)'][0]
      debug('Neo4j#getNodeCount', nodeLabel, count)
      cb(null, count)
    }
  })
}

Neo4j.prototype.getNodes = function (start, steps, cb) {
  debug('Neo4j#getNodes', start, steps)
  // FIXME(bkendall): this is a pretty poor way to iterate this list.
  var nodes = [ 'a', 'b', 'c', 'd', 'e', 'f', 'g' ]
  var node = nodes.shift()
  var query = 'MATCH (' + node + ':' + start.label + ')'
  var returnVars = [node]
  steps.forEach(function (step) {
    var s
    var edge = nodes.shift()
    node = nodes.shift()
    if (step.Out) {
      s = step.Out
      query += '-[' + edge + ':' + s.edge.label + ']->(' + node + ':' + s.node.label + ')'
      returnVars.push(edge, node)
    } else if (step.In) {
      s = step.In
      query += '<-[' + edge + ':' + s.edge.label + ']-(' + node + ':' + s.node.label + ')'
      returnVars.push(edge, node)
    }
    var props
    if (s && s.node && s.node.props) {
      s.node.propsName = props = node + 'Props' // e.g., cProps
      s.node.propsAsQuery = Object.keys(s.node.props).map(function (key) {
        return node + '.' + key + '={' + props + '}.' + key
      })
    }
    if (s && s.edge && s.edge.props) {
      s.edge.propsName = props = edge + 'Props'
      s.edge.propsAsQuery = Object.keys(s.edge.props).map(function (key) {
        return edge + '.' + key + '={' + props + '}.' + key
      })
    }
  })
  var params = {
    props: start.props
  }
  var where = []
  if (start.props) {
    Object.keys(start.props).forEach(function (key) {
      where.push('a.' + key + '={props}.' + key)
    })
  }
  steps.forEach(function (step) {
    if (keypather.get(step, 'In.edge.props')) {
      where = where.concat(step.In.edge.propsAsQuery)
      params[step.In.edge.propsName] = step.In.edge.props
    } else if (keypather.get(step, 'Out.edge.props')) {
      where = where.concat(step.Out.edge.propsAsQuery)
      params[step.Out.edge.propsName] = step.Out.edge.props
    }
    if (keypather.get(step, 'In.node.props')) {
      where = where.concat(step.In.node.propsAsQuery)
      params[step.In.node.propsName] = step.In.node.props
    } else if (keypather.get(step, 'Out.node.props')) {
      where = where.concat(step.Out.node.propsAsQuery)
      params[step.Out.node.propsName] = step.Out.node.props
    }
  })
  var q = [query]
  if (where.length) { q.push('WHERE ' + where.join(' AND ')) }
  q.push('RETURN ' + returnVars.join(','))
  q = q.join('\n')

  this._query(q, params, function (err, data) {
    if (err) {
      cb(err)
    } else if (!data) {
      cb(null, null)
    } else {
      var d = data[last(returnVars)] || []
      debug('Neo4j#getNodes', data, d)
      cb(err, d, data)
    }
  })
}

Neo4j.prototype.writeConnections = function (connections, cb) {
  debug('Neo4j#writeConnections', connections)
  if (connections.length === 0) { return cb(null) }
  var self = this
  async.mapSeries(
    connections,
    function (conn, mapCb) {
      self._createUniqueRelationship(conn[0], conn[1], conn[2], mapCb)
    },
    cb)
}

Neo4j.prototype.writeConnection = Neo4j.prototype._createUniqueRelationship = function (start, relationship, end, cb) {
  debug('Neo4j#writeConnection', start, relationship, end)
  var q = [
    'MATCH (a:' + start.label + ' {id: {startProps}.id}),' +
    '(b:' + end.label + ' {id: {endProps}.id})',
    'MERGE (a)-[r:' + relationship.label + ']->(b)'
  ]

  if (relationship.props) {
    var ps = Object.keys(relationship.props).map(function (prop) {
      return 'r.' + prop + "='" + relationship.props[prop] + "'"
    }).join(', ')
    q.push('ON CREATE SET ' + ps)
    q.push('ON MATCH SET ' + ps)
  }

  q.push('RETURN a,r,b')
  q = q.join('\n')

  var p = {
    startProps: start.props,
    endProps: end.props
  }

  this._query(q, p, function (err, data) {
    if (err) {
      debug('Neo4j#writeConnection', err)
      cb(err)
    } else if (!data.a || !data.r || !data.b) {
      var notCreatedErr = new Error('relationship was not created in Neo4j')
      debug('Neo4j#writeConnection', notCreatedErr, data)
      cb(notCreatedErr)
    } else {
      cb(null)
    }
  })
}

Neo4j.prototype.deleteConnections = function (connections, cb) {
  debug('Neo4j#deleteConnections', connections)
  var self = this
  if (connections.length === 0) { return cb(null) }
  async.mapSeries(
    connections,
    function (conn, mapCb) {
      self._deleteConnection(
        { id: conn.subject },
        conn.predicate,
        { id: conn.object },
        mapCb)
    }, cb)
}

Neo4j.prototype.deleteConnection = Neo4j.prototype._deleteConnection = function (start, relationshipLabel, end, cb) {
  debug('Neo4j#deleteConnection', start, relationshipLabel, end)
  var q = [
    'MATCH (a:' + start.label + ' {id: {startProps}.id})-' +
    '[r:' + relationshipLabel + ']->' + '(b:' + end.label + ' {id: {endProps}.id})',
    'DELETE r'
  ].join('\n')
  var p = {
    startProps: start.props,
    endProps: end.props
  }
  this._query(q, p, cb)
}

Neo4j.prototype.writeNodes = function (nodes, cb) {
  debug('Neo4j#writeNodes', nodes)
  var self = this
  async.forEach(
    nodes,
    function (n, eachCb) { self._writeUniqueNode(n, eachCb) },
    cb)
}

Neo4j.prototype.writeNode = Neo4j.prototype._writeUniqueNode = function (node, cb) {
  debug('Neo4j#writeNode', node)
  var nodeProps = []
  Object.keys(node.props).forEach(function (key) {
    if (key !== 'id') {
      nodeProps.push('n.' + key + ' = {props}.' + key)
    }
  })
  var q = [
    'MERGE (n:' + node.label + ' {id: {props}.id})'
  ]
  if (nodeProps.length) {
    nodeProps = nodeProps.join(', ')
    q.push('ON CREATE SET ' + nodeProps)
    q.push('ON MATCH SET ' + nodeProps)
  }
  q.push('RETURN n')
  q = q.join('\n')
  var p = {
    props: node.props
  }
  this._query(q, p, function (err, data) {
    if (err) {
      debug('Neo4j#writeNode', err)
      cb(err)
    } else if (!data || !data.n) {
      var notCreatedErr = new Error('node was not created in Neo4j')
      debug('Neo4j#writeNode', notCreatedErr, data)
      cb(notCreatedErr)
    } else {
      cb(null)
    }
  })
}

Neo4j.prototype.deleteNodeAndConnections = function (node, cb) {
  debug('Neo4j#deleteNodeAndConnections', node)
  var q = [
    'MATCH (n:' + node.label + ' {id: {props}.id})',
    'OPTIONAL MATCH (n)-[r]-()',
    'DELETE n,r'
  ].join('\n')
  var p = {
    props: node.props
  }
  this._query(q, p, cb)
}

Neo4j.prototype._query = function (q, p, cb) {
  debug('Neo4j#_query', backlight.escape(q), p)

  // write data to transaction
  var t = this.cypher.transaction()
  t.write({
    statement: q,
    parameters: p
  })

  // prepare handlers
  var err = null
  var data = {}
  t.on('error', function (e) {
    debug('Neo4j#_query error', e)
    err = e
  })
  t.on('data', function (d) {
    if (d) {
      Object.keys(d).forEach(function (key) {
        if (!data[key]) {
          data[key] = [d[key]]
        } else {
          data[key].push(d[key])
        }
      })
    }
  })
  t.on('end', function () {
    cb(err, data)
  })

  // send the transaction
  t.commit()
}
