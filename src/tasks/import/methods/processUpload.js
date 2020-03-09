/**
 * Method to dispatch downstream file import methods given an upload.
 */

const pick = require('lodash/pick')
const { idRandom } = require('../../../lib/utils')

const SPEC_DEFAULTS = {}

async function processUpload(req, ctx) {
  const { fileImportService, logger } = ctx
  const spec = Object.assign({}, SPEC_DEFAULTS, req.spec)
  const { upload } = spec

  if (!upload) throw new Error('Spec incomplete')

  /*
    Dispatch import request to fetch files or manifest.
   */

  const now = new Date()
  const { _id: id } = upload
  const method = upload.spec ? 'fetchFiles' : 'fetchManifest'
  const importId = `${method}-${id}-${now.getTime()}-${idRandom()}`

  logger.info('Dispatching import', { importId })

  await fileImportService.create({
    _id: importId,
    method,
    dispatch_at: now,
    dispatch_key: id,
    expires_at: new Date(now.getTime() + 86400000), // 24 hours from now
    spec
  })

  return { upload }
}

module.exports = async (...args) => {
  try {
    return await processUpload(...args)
  } catch (err) {
    // Wrap errors, ensure they are written to the store
    return {
      error: pick(err, ['code', 'className', 'message', 'type'])
    }
  }
}
