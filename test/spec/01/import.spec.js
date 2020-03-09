/**
 * Tests for import tasks
 */

const feathers = require('@feathersjs/feathers')
const auth = require('@feathersjs/authentication-client')
const localStorage = require('localstorage-memory')
const restClient = require('@feathersjs/rest-client')
const axios = require('axios')
const murmurHash3 = require('murmurhash3js')

describe('import tasks', function() {
  this.timeout(120000)

  const now = new Date()
  const hostname = 'test-hostname-0'
  const hostParts = hostname.split('-')

  const model = {
    props: {
      storage_options: {
        local: {
          path: './test/data/import/drop'
        }
      },
      temp_path: './test/data/import/temp'
    },
    state: {
      _id: 'taskMachine-import-current',
      source_defaults: {
        some_default: 'default'
      },
      sources: [
        {
          description: 'Import files based on a method',
          // NOTE: Deprecated in favor of consistent hashing
          // queue_group: 'dendra.fileImport.v2',
          pub_to_subject: '{org_slug}.importRecords.out',
          sub_options: {
            ack_wait: 3600000,
            durable_name: '20181223'
          },
          sub_to_subject: 'dendra.fileImport.v2.req.{hostOrdinal}'
        }
      ],
      created_at: now,
      updated_at: now
    }
  }

  const requestSubject = 'dendra.fileImport.v2.req.0'
  const testName = 'dendra-worker-tasks-file UNIT_TEST'
  const testFile = 'TOA5_EastFace_77409.SCIsoilH2O'

  const id = {}
  const webConnection = {}

  const authWebConnection = async () => {
    const cfg = main.app.get('connections').web
    const storageKey = (webConnection.storageKey = murmurHash3.x86.hash128(
      `TEST,${cfg.url}`
    ))
    const app = (webConnection.app = feathers()
      .configure(restClient(cfg.url).axios(axios))
      .configure(
        auth({
          storage: localStorage,
          storageKey
        })
      ))

    await app.authenticate(cfg.auth)
  }
  const removeDocuments = async (path, query) => {
    const res = await webConnection.app.service(path).find({ query })

    for (const doc of res.data) {
      await webConnection.app.service(path).remove(doc._id)
    }
  }
  const cleanup = async () => {
    await removeDocuments('/uploads', {
      'spec.comment': testName
    })
    await removeDocuments('/stations', {
      name: testName
    })
    await removeDocuments('/organizations', {
      name: testName
    })
  }

  Object.defineProperty(model, '$app', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: main.app
  })
  Object.defineProperty(model, 'hostname', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: hostname
  })
  Object.defineProperty(model, 'hostOrdinal', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: hostParts[hostParts.length - 1]
  })
  Object.defineProperty(model, 'key', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: 'import'
  })
  Object.defineProperty(model, 'private', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: {}
  })

  let tasks
  let machine
  let upload
  let fileImport

  before(async function() {
    await authWebConnection()
    await cleanup()

    id.org = (
      await webConnection.app.service('/organizations').create({
        name: testName,
        slug: 'dendra-worker-tasks-file'
      })
    )._id

    id.station = (
      await webConnection.app.service('/stations').create({
        is_active: true,
        is_enabled: true,
        is_stationary: true,
        name: testName,
        organization_id: id.org,
        station_type: 'weather',
        time_zone: 'PST',
        utc_offset: -28800
      })
    )._id

    id.upload = (
      await webConnection.app.service('/uploads').create({
        organization_id: id.org,
        spec: {
          comment: testName,
          method: 'csvStream',
          options: {
            context: {
              station: 'dendra_worker_tasks_file_station',
              table: 'dendra_worker_tasks_file_table'
            },
            from_line: 2,
            skip_columns: ['RECORD'],
            skip_lines: {
              at: [3, 4]
            },
            time_adjust: 28800
          }
        },
        spec_type: 'file/import',
        station_id: id.station,
        storage: {
          method: 'local',
          options: {
            file_name: testFile
          }
        }
      })
    )._id
  })

  after(async function() {
    await cleanup()

    await Promise.all([
      model.private.stan
        ? new Promise((resolve, reject) => {
            model.private.stan.removeAllListeners()
            model.private.stan.once('close', resolve)
            model.private.stan.once('error', reject)
            model.private.stan.close()
          })
        : Promise.resolve()
    ])
  })

  it('should import', function() {
    tasks = require('../../../dist').import

    expect(tasks).to.have.property('sources')
  })

  it('should create machine', function() {
    machine = new tm.TaskMachine(model, tasks, {
      helpers: {
        logger: console
      },
      interval: 500
    })

    expect(machine).to.have.property('model')
  })

  it('should run', function() {
    model.scratch = {}

    return machine
      .clear()
      .start()
      .then(success => {
        /* eslint-disable-next-line no-unused-expressions */
        expect(success).to.be.true

        // Verify task state
        expect(model).to.have.property('sourcesReady', true)
        expect(model).to.have.property('stanCheckReady', false)
        expect(model).to.have.property('stanCloseReady', false)
        expect(model).to.have.property('stanReady', true)
        expect(model).to.have.property('subscriptionsReady', true)
        expect(model).to.have.property('versionTsReady', false)

        // Check for defaults
        expect(model).to.have.nested.property(
          'sources.dendra_fileImport_v2_req__hostOrdinal_.some_default',
          'default'
        )
      })
  })

  it('should get upload using _id', function() {
    return webConnection.app
      .service('/uploads')
      .get(id.upload)
      .then(doc => {
        expect(doc).to.have.property('_id', id.upload)

        upload = doc
      })
  })

  it('should process processUpload request', function() {
    const service = main.app
      .get('connections')
      .dispatch.app.service('/file-imports')

    service.store = {} // HACK: Reset store before test

    const msgStr = JSON.stringify({
      _id: 'process-upload-1234',
      method: 'processUpload',
      spec: {
        upload
      }
    })

    return new Promise((resolve, reject) => {
      model.private.stan.publish(requestSubject, msgStr, (err, guid) =>
        err ? reject(err) : resolve(guid)
      )
    })
  })

  it('should wait for 5 seconds', function() {
    return new Promise(resolve => setTimeout(resolve, 5000))
  })

  it('should verify file imports after processUpload', function() {
    return main.app
      .get('connections')
      .dispatch.app.service('/file-imports')
      .find()
      .then(res => {
        expect(res)
          .to.have.property('data')
          .lengthOf(1)

        // Spot check the first one
        expect(res).to.have.nested.property('data.0.dispatch_key', upload._id)
        expect(res).to.have.nested.property('data.0.method', 'fetchFiles')

        fileImport = res.data[0]
      })
  })

  it('should process fetchFiles request', function() {
    const service = main.app
      .get('connections')
      .dispatch.app.service('/file-imports')

    service.store = {} // HACK: Reset store before test

    const msgStr = JSON.stringify({
      _id: 'process-upload-1234',
      method: 'fetchFiles',
      spec: fileImport.spec
    })

    return new Promise((resolve, reject) => {
      model.private.stan.publish(requestSubject, msgStr, (err, guid) =>
        err ? reject(err) : resolve(guid)
      )
    })
  })

  it('should wait for 5 seconds', function() {
    return new Promise(resolve => setTimeout(resolve, 5000))
  })

  it('should verify file imports after fetchFiles', function() {
    return main.app
      .get('connections')
      .dispatch.app.service('/file-imports')
      .find()
      .then(res => {
        expect(res)
          .to.have.property('data')
          .lengthOf(1)

        // Spot check the first one
        expect(res).to.have.nested.property('data.0.dispatch_key', upload._id)
        expect(res).to.have.nested.property('data.0.method', 'processFiles')

        fileImport = res.data[0]
      })
  })

  it('should process processFiles request', function() {
    const service = main.app
      .get('connections')
      .dispatch.app.service('/file-imports')

    service.store = {} // HACK: Reset store before test

    const msgStr = JSON.stringify({
      _id: 'process-upload-1234',
      method: 'processFiles',
      spec: fileImport.spec
    })

    return new Promise((resolve, reject) => {
      model.private.stan.publish(requestSubject, msgStr, (err, guid) =>
        err ? reject(err) : resolve(guid)
      )
    })
  })

  it('should wait for 10 seconds', function() {
    return new Promise(resolve => setTimeout(resolve, 10000))
  })

  it('should verify file imports after processFiles', function() {
    return main.app
      .get('connections')
      .dispatch.app.service('/file-imports')
      .find()
      .then(res => {
        expect(res)
          .to.have.property('data')
          .lengthOf(0)
      })
  })

  it('should verify upload after processFiles', function() {
    return webConnection.app
      .service('/uploads')
      .get(id.upload)
      .then(doc => {
        expect(doc).to.have.property('_id', id.upload)
        expect(doc).to.have.nested.property(
          'result.processed.0.file.name',
          `${testFile}.dat`
        )
        expect(doc).to.have.nested.property('result.processed.0.file.path')
        expect(doc).to.have.nested.property(
          'result.processed.0.stats.publish_count',
          30
        )
        expect(doc).to.have.nested.property(
          'result.processed.0.stats.publish_error_count',
          0
        )
        expect(doc).to.have.nested.property(
          'result.processed.0.stats.record_count',
          30
        )
        expect(doc).to.have.nested.property(
          'result.processed.0.stats.skipped_record_count',
          2
        )
        expect(doc).to.have.nested.property(
          'result.processed.0.stats.time_max',
          1509602400000
        )
        expect(doc).to.have.nested.property(
          'result.processed.0.stats.time_min',
          1509576300000
        )
      })
  })
})
