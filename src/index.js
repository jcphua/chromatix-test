const fs = require('fs'),
      path = require('path'),
      csvParser = require('csv-parser'),
      readline = require('readline');

const _csv_filenames = ['test500', 'test4000', 'node-data-processing-medium-data'],
      csv_filename = _csv_filenames[0],
      csv_path = path.join(__dirname, `../data/${ csv_filename }.csv`);

let csv_data = [],
    _regions = []
    _data = {};

// readline.createInterface({ input: process.stdin, output: process.stdout });

console.log(`Data source:\n'${ csv_path }'`);

console.time('readstream');
fs.createReadStream(csv_path)
    .on('error', () => {
        console.error(arguments);
    })
    .pipe(csvParser())
    .on('data', (row) => {
        // console.log(row);
        csv_data.push(row);

        if (!_regions.includes(row['Region'])) { _regions.push(row['Region']); }
    })
    .on('end', () => {
        console.timeEnd('readstream');
        console.log(`${ csv_data.length } records processed.`);

        console.log(_regions.sort());
    });