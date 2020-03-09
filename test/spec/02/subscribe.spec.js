/**
 * Tests for subscribing to imported records
 */

const STAN = require('node-nats-streaming')

describe('Subscribe to imported records', function() {
  this.timeout(30000)

  let messages
  let stan
  let sub

  before(function() {
    const cfg = main.app.get('clients').stan
    stan = STAN.connect(cfg.cluster, 'test-file-subscribe', cfg.opts || {})

    return new Promise((resolve, reject) => {
      stan.once('connect', () => {
        resolve(stan)
      })
      stan.once('error', err => {
        reject(err)
      })
    }).then(() => {
      return new Promise(resolve => setTimeout(resolve, 1000))
    })
  })

  after(function() {
    return Promise.all([
      stan
        ? new Promise((resolve, reject) => {
            stan.removeAllListeners()
            stan.once('close', resolve)
            stan.once('error', reject)
            stan.close()
          })
        : Promise.resolve()
    ])
  })

  it('should subscribe', function() {
    const opts = stan.subscriptionOptions()
    opts.setDeliverAllAvailable()
    opts.setDurableName('importRecords')

    sub = stan.subscribe('dendra-worker-tasks-file.importRecords.out', opts)
    messages = []
    sub.on('message', msg => {
      messages.push(JSON.parse(msg.getData()))
    })
  })

  it('should wait for 5 seconds to collect messages', function() {
    return new Promise(resolve => setTimeout(resolve, 5000))
  })

  it('should have imported messages', function() {
    sub.removeAllListeners()

    expect(messages).to.have.lengthOf(30)

    expect(messages).to.have.nested.property(
      '0.context.station',
      'dendra_worker_tasks_file_station'
    )
    expect(messages).to.have.nested.property(
      '0.context.table',
      'dendra_worker_tasks_file_table'
    )
    expect(messages).to.have.nested.property('0.context.imported_at')
    expect(messages).to.have.nested.property('0.context.file.name')
    expect(messages).to.have.nested.property(
      '0.context.req_id',
      'process-upload-1234'
    )
    expect(messages).to.have.nested.property('0.context.upload_id')
    expect(messages).to.have.nested.property(
      '0.context.org_slug',
      'dendra-worker-tasks-file'
    )
    expect(messages).to.have.nested.property('0.payload.VW1', 0.049)
    expect(messages).to.have.nested.property('0.payload.Tension2', null)
    expect(messages).to.have.nested.property('0.payload.time', 1509576300000)
  })
})
