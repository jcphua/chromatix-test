const fs = require('fs'),
      path = require('path'),
      csvParser = require('csv-parser'),
      readline = require('readline');

const csv_filenames = ['test500', 'test4000', 'node-data-processing-medium-data'],
      csv_filename = csv_filenames[0],
      csv_path = path.join(__dirname, `../data/${ csv_filename }.csv`);

let csv_data = [],
    _data = {},
    _regions = {},
    region_names = [],
    row_ctr = 0;

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

        const region = row['Region'];
        if (!_regions[region]) { 
            _regions[region] = { _indexes: [], countries: {} }; 
        }
        _regions[region]._indexes.push(row_ctr);

        if (!region_names.includes(region)) { region_names.push(region); }
        // if (!_data[region]) { _data[region] }

        row_ctr++;
    })
    .on('end', () => {

        // console.log(_region_names.sort());
        // console.log(_regions);

        console.timeEnd('readstream');
        console.log(`${ csv_data.length } records processed.\n`);

        _regions = _fns_tasks.gen_revenueCostProfit_region(csv_data, _regions);

        // console.log(_regions);
        // console.log(JSON.stringify(_regions, false, 2));
    });

const _fns_tasks = {
    gen_revenueCostProfit_region: (_data, _regions) => {
        console.time('regions');
        for (let region in _regions) {
        // for (let i=0; i<Object.keys(_regions).length; i++) {
        //     let region = _regions[i] || Object.keys(_regions)[i];
            console.time(`region: ${region}`);
            let _region = _regions[region];
            // console.log(_region);
            for (let idx = 0; idx < _region._indexes.length; idx++) {
                let row = _data[_region._indexes[idx]];
                // console.log(row);
                if (!_region.countries[row['Country']]) { _region.countries[row['Country']] = { _indexes: [] }; }
                _region.countries[row['Country']]._indexes.push(_region._indexes[idx]);
            }
            console.timeEnd(`region: ${region}`);
        }
        console.timeEnd('regions');
        return _regions;
    },
    gen_priorityOrders_date: () => {

    },
    gen_avgDaysToShip_date: () => {

    }
};