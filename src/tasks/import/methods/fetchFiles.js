/**
 * Method to copy files from storage to temp for processing.
 */

const pick = require('lodash/pick')
const storages = require('../../../lib/storages')
const { getAuthUser } = require('../../../lib/helpers')
const { idRandom } = require('../../../lib/utils')

const SPEC_DEFAULTS = {}

async function fetchFiles(req, ctx) {
  const {
    fileImportService,
    logger,
    organizationService,
    storageOptions,
    tempPath
  } = ctx
  const spec = Object.assign({}, SPEC_DEFAULTS, req.spec)
  const { upload } = spec

  if (!upload) throw new Error('Spec incomplete')

  /*
    Create storage and get files.
   */

  const { _id: id, spec: loadSpec, storage: storageSpec } = upload

  if (!loadSpec) throw new Error('Load spec missing')
  if (!storageSpec) throw new Error('Storage spec missing')

  const storageMethod = storageSpec.method

  logger.info('Creating storage', { storageMethod })

  const StorageClass = storages[storageMethod]

  if (!StorageClass) throw new Error('Storage not supported')

  const now = new Date()
  const tempDir = `${id}-${now.getTime()}-${idRandom()}`
  const storage = new StorageClass(storageOptions[storageMethod])
  const files = await storage.getFiles(storageSpec.options, tempPath, tempDir)

  /*
    Authenticate and/or verify user credentials.
   */

  await getAuthUser(ctx)

  /*
    Fetch the organization.
   */

  logger.info('Getting organization', { _id: upload.organization_id })

  const organization = await organizationService.get(upload.organization_id)

  /*
    Dispatch import method to process gotten files.
   */

  const method = 'processFiles'
  const importId = `${method}-${tempDir}`
  const importSpec = {
    files,
    organization,
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

  return { files, upload }
}

module.exports = async (...args) => {
  try {
    return await fetchFiles(...args)
  } catch (err) {
    // Wrap errors, ensure they are written to the store
    return {
      error: pick(err, ['code', 'className', 'message', 'type'])
    }
  }
}
