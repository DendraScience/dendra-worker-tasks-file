/**
 * Method to process files in temp.
 */

const fs = require('fs').promises
const path = require('path')
const pick = require('lodash/pick')
const importers = require('../../../lib/importers')
const { getAuthUser } = require('../../../lib/helpers')
const { idRandom } = require('../../../lib/utils')

const SPEC_DEFAULTS = {}

async function processFiles(req, ctx) {
  const { fileImportService, logger, uploadService } = ctx
  const spec = Object.assign({}, SPEC_DEFAULTS, req.spec)
  const { files, organization, result = {}, upload } = spec

  if (!(files && upload)) throw new Error('Spec incomplete')

  const { _id: id, spec: loadSpec } = upload

  if (!loadSpec) throw new Error('Load spec missing')

  /*
    Create and run importer.
   */

  const loadMethod = loadSpec.method

  logger.info('Running importer', { loadMethod })

  const importer = importers[loadMethod]

  if (!importer) throw new Error('Importer not supported')

  let runRes
  try {
    runRes = await importer.run(req, ctx, spec, file => {
      const tempPath = path.dirname(file.path)
      return fs
        .unlink(file.path)
        .then(() => fs.rmdir(tempPath))
        .catch(err => logger.warn('Temp rmdir error', { err, tempPath }))
    })
  } catch (err) {
    runRes = { files: [], processed: [{ error: err.message }] }
  }

  result.processed = result.processed
    ? result.processed.concat(runRes.processed)
    : runRes.processed

  /*
    Dispatch import request for additional files, or finish up.
   */

  if (runRes.files.length) {
    const now = new Date()
    const method = 'processFiles'
    const importId = `${method}-${id}-${now.getTime()}-${idRandom()}`
    const importSpec = {
      files: runRes.files,
      organization,
      result,
      upload
    }

    logger.info('Dispatching import', { importId })

    await fileImportService.create({
      _id: importId,
      method,
      dispatch_at: now,
      dispatch_key: id,
      expires_at: new Date(now.getTime() + 86400000), // 24 hours from now
      spec: importSpec
    })
  } else {
    /*
      Authenticate and/or verify user credentials.
     */

    await getAuthUser(ctx)

    logger.info('Patching upload', { _id: id })

    await uploadService.patch(id, {
      $set: { result }
    })
  }

  return runRes
}

module.exports = async (...args) => {
  try {
    return await processFiles(...args)
  } catch (err) {
    // Wrap errors, ensure they are written to the store
    return {
      error: pick(err, ['code', 'className', 'message', 'type'])
    }
  }
}
