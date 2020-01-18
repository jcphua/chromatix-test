const fs = require('fs'),
      path = require('path'),
      csvParser = require('csv-parser'),
      readline = require('readline');

const csv_filenames = ['test500', 'test4000', 'node-data-processing-medium-data'],
      csv_filename = csv_filenames[0],
      csv_path = path.join(__dirname, `../data/${ csv_filename }.csv`);

const __indexes = { _indexes: [] },
      __totals = { Total: { Revenue: 0, Cost: 0, Profit: 0 } };

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
            _regions[region] = { ...__totals, countries: {}, _indexes: [] }; 
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

        console.time('\nwrite file');
        fs.writeFileSync(path.join(__dirname, '../output/_data.json'), JSON.stringify(_regions));
        console.timeEnd('\nwrite file');
    });

const _fns_tasks = {
    gen_revenueCostProfit_region: (_data, _regions) => {
        console.time('\nAll regions');
        for (let region in _regions) {
        // for (let i=0; i<Object.keys(_regions).length; i++) {
        //     let region = _regions[i] || Object.keys(_regions)[i];
            console.time(`Region: ${region}`);
            let _region = _regions[region];
            for (let idx = 0; idx < _region._indexes.length; idx++) {
                let row_index = _region._indexes[idx],
                    _row = _data[row_index],
                    country_name = _row['Country'];

                if (!_region.countries[country_name]) { 
                    _region.countries[country_name] = { 
                        'Total': {
                            'Revenue': 0,
                            'Cost': 0,
                            'Profit': 0
                        },
                        _indexes: []
                    };
                }
                _region.countries[country_name]._indexes.push(row_index);
                
                _region.countries[country_name]['Total']['Revenue'] += parseFloat(_row['Total Revenue']);
                _region.countries[country_name]['Total']['Cost'] += parseFloat(_row['Total Cost']);
                _region.countries[country_name]['Total']['Profit'] += parseFloat(_row['Total Profit']);

                _region['Total']['Revenue'] += parseFloat(_row['Total Revenue']);
                _region['Total']['Cost'] += parseFloat(_row['Total Cost']);
                _region['Total']['Profit'] += parseFloat(_row['Total Profit']);
            }

            console.timeEnd(`Region: ${region}`);
        }
        console.timeEnd('\nAll regions');
        return _regions;
    },
    gen_priorityOrders_date: () => {

    },
    gen_avgDaysToShip_date: () => {

    }
};