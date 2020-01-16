const fs = require('fs'),
      path = require('path'),
      readline = require('readline'),
      csvParser = require('csv-parser');

const _csv_filenames = ['test500', 'test4000', 'node-data-processing-medium-data'],
      csv_filename = _csv_filenames[0],
      csv_path = path.join(__dirname, `../data/${ csv_filename }.csv`);

let data = [];

console.log(`Using following file as data source:\n'${ csv_path }'`);

fs.createReadStream(csv_path)
    .on('error', () => {
        console.error(arguments);
    })
    .pipe(csvParser())
    .on('data', (row) => {
        // console.log(row);
        data.push(row);
    })
    .on('end', () => {
        console.log(`${ data.length } records processed.`);

        // readline
    });