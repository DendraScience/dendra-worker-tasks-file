/**
 * Process an individual message.
 */

const methods = require('./methods')

async function processItem({ data, dataObj, msgSeq }, ctx) {
  const { logger, subSubject } = ctx
  try {
    /*
      Validate import method.
     */

    if (!dataObj.method) throw new Error('Import method undefined')

    const method = methods[dataObj.method]

    if (!method) throw new Error('Import method not supported')

    /*
      Invoke inport method.
     */

    const startedAt = new Date()
    const importRes = await method(dataObj, ctx)
    const finishedAt = new Date()

    if (!importRes) throw new Error('Import result undefined')

    logger.info('Import', {
      importRes,
      msgSeq,
      subSubject,
      startedAt,
      finishedAt
    })
  } catch (err) {
    logger.error('Processing error', { msgSeq, subSubject, err, dataObj })
  }
}

module.exports = processItem
