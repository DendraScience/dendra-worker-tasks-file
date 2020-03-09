"use strict";

/**
 * CSV stream importer.
 */
const fs = require('fs');

const moment = require('moment');

const parse = require('csv-parse');

const pick = require('lodash/pick');

async function run(req, ctx, spec, deleteCb) {
  const {
    pubToSubj,
    stan
  } = ctx;
  const {
    files,
    organization,
    upload
  } = spec;
  const {
    _id: id,
    spec: loadSpec
  } = upload;
  const file = files.shift();
  if (!file) throw new Error('No files to process');
  const importedAt = new Date();
  const stats = {
    imported_at: importedAt,
    publish_count: 0,
    publish_error_count: 0,
    record_count: 0,
    skipped_record_count: 0
  };
  const options = loadSpec.options || {};
  const {
    cast_nan: castNaN = true,
    cast_null: castNull = false,
    context = {},
    skip_columns: skipColumns = [],
    skip_lines: skipLines = {},
    time_adjust: timeAdjust,
    time_column: timeColumn,
    time_format: timeFormat
  } = options;
  const nanValues = castNaN === true ? ['NAN', 'NaN'] : typeof castNaN === 'string' ? [castNaN] : Array.isArray(castNaN) ? castNaN : [];
  const nullValues = castNull === true ? ['NULL', 'null'] : typeof castNull === 'string' ? [castNull] : Array.isArray(castNull) ? castNull : [];
  const timeColumns = typeof timeColumn === 'string' ? [timeColumn] : ['TIME', 'time', 'TIMESTAMP', 'timestamp']; // On data handler

  const onData = data => {
    /*
      Prepare outbound messages and publish.
     */
    context.imported_at = importedAt;
    context.file = file;
    context.req_id = req._id;
    context.upload_id = id;
    if (!context.org_slug && organization.slug) context.org_slug = organization.slug;
    const pubSubject = pubToSubj.replace(/{([.\w]+)}/g, (_, k) => context[k]);
    const msgStr = JSON.stringify({
      context,
      payload: data
    });
    stan.publish(pubSubject, msgStr, (err, guid) => {
      if (err) stats.publish_error_count++;else stats.publish_count++; // DEBUG
      // if (err) logger.error('Publish error', { pubSubject, err })
      // else logger.info('Published', { pubSubject, guid })
    });
  }; // On record handler


  const onRecord = (record, {
    lines
  }) => {
    let newRecord = Object.assign({}, record); // Check for skipped lines

    if (skipLines) {
      const {
        at,
        from,
        to
      } = skipLines;
      if (Array.isArray(at) && at.includes(lines)) newRecord = null;
      if (typeof from === 'number' && lines >= from) newRecord = null;
      if (typeof to === 'number' && lines <= to) newRecord = null;
    }

    if (newRecord) {
      // Check for skipped columns
      if (skipColumns) skipColumns.forEach(name => delete newRecord[name]); // Cast time column

      const timeColFound = timeColumns.find(name => newRecord[name] !== undefined);
      if (!timeColFound) throw new Error('Time column not found');
      let time = newRecord[timeColFound];
      time = typeof timeFormat === 'string' ? moment.utc(time, timeFormat) : moment.utc(time);
      if (typeof timeAdjust === 'number') time.add(timeAdjust, 's');
      if (!time.isValid()) throw new Error('Time value not valid');
      delete newRecord[timeColFound];
      newRecord.time = time.valueOf();
      stats.time_max = stats.time_max === undefined ? newRecord.time : Math.max(stats.time_max, newRecord.time);
      stats.time_min = stats.time_min === undefined ? newRecord.time : Math.min(stats.time_min, newRecord.time); // Cast NaN and null

      Object.keys(newRecord).forEach(name => {
        if (name === timeColFound) {} else if (nanValues.includes(newRecord[name])) newRecord[name] = NaN;else if (nullValues.includes(newRecord[name])) newRecord[name] = null;
      });
      stats.record_count++;
    } else {
      stats.skipped_record_count++;
    }

    return newRecord;
  };
  /*
    Configure parser.
   */
  // TODO: Implement columns as a map


  const parseOptions = Object.assign({
    cast: true,
    columns: true,
    on_record: onRecord
  }, pick(options, [// SEE: https://csv.js.org/parse/options/
  'bom', 'columns', 'comment', 'delimiter', 'escape', 'from', 'from_line', 'ltrim', 'max_record_size', 'quote', 'relax', 'relax_column_count', 'relax_column_count_less', 'relax_column_count_more', 'record_delimiter', 'rtrim', 'skip_empty_lines', 'skip_lines_with_error', 'skip_lines_with_empty_values', 'to', 'to_line', 'trim']));
  const readStream = fs.createReadStream(file.path);
  const parser = readStream.pipe(parse(parseOptions));
  await new Promise((resolve, reject) => {
    parser.on('data', onData).once('end', resolve).once('error', reject);
  }).finally(() => {
    parser.removeAllListeners();
    readStream.unpipe();
  });
  if (deleteCb) await deleteCb(file);
  return {
    files,
    processed: [{
      file,
      stats
    }]
  };
}

module.exports = {
  run
};