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

        _regions = _fns_tasks.gen_region_revenueCostProfit(csv_data, _regions);
        _data['Regions'] = _regions;
        _data = _fns_tasks.get_region_revenueCostProfit_itemTypes(csv_data, _regions);

        console.time('\nwrite file');
        fs.writeFileSync(path.join(__dirname, '../output/_data.json'), JSON.stringify(_data));
        console.timeEnd('\nwrite file');
    });

const _fns_tasks = {
    gen_region_revenueCostProfit: (_data, _regions) => {
        console.time('\nAll region Totals');
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
                        'ItemTypes': {},
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
        console.timeEnd('\nAll region Totals');
        return _regions;
    },
    get_region_revenueCostProfit_itemTypes: (_data, _regions) => {
        console.time('\nAll region ItemTypes');
        let _item_types = {};
        for (let region in _regions) {
        // for (let i=0; i<Object.keys(_regions).length; i++) {
        //     let region = _regions[i] || Object.keys(_regions)[i];
            console.time(`Region: ${region}`);
            let _region = _regions[region];
            for (let country in _region.countries) {
                let _country = _region.countries[country];

                for (let idx = 0; idx < _country._indexes.length; idx++) {
                    let row_index = _country._indexes[idx],
                        _row = _data[row_index],
                        item_type = _row['Item Type'];

                    if (!_country['ItemTypes'][item_type]) { 
                        _country['ItemTypes'][item_type] = {
                            'Total': {
                                'Revenue': 0,
                                'Cost': 0,
                                'Profit': 0
                            },
                            _indexes: []
                        }
                    }
                    _country['ItemTypes'][item_type]._indexes.push(row_index);
                    
                    _country['ItemTypes'][item_type]['Total']['Revenue'] += parseFloat(_row['Total Revenue']);
                    _country['ItemTypes'][item_type]['Total']['Cost'] += parseFloat(_row['Total Cost']);
                    _country['ItemTypes'][item_type]['Total']['Profit'] += parseFloat(_row['Total Profit']);

                    if (!_item_types[item_type]) {
                        _item_types[item_type] = {
                            'Revenue': 0,
                            'Cost': 0,
                            'Profit': 0,
                            _indexes: []
                        }
                    }
                    _item_types[item_type]._indexes.push(row_index);
                    _item_types[item_type]['Revenue'] += parseFloat(_row['Total Revenue']);
                    _item_types[item_type]['Cost'] += parseFloat(_row['Total Cost']);
                    _item_types[item_type]['Profit'] += parseFloat(_row['Total Profit']);
                }
            }

            console.timeEnd(`Region: ${region}`);
        }
        console.timeEnd('\nAll region ItemTypes');
        return {
            'Regions': _regions,
            'Item Types': _item_types
        };
    },
    gen_date_priorityOrders: () => {

    },
    gen_date_avgDaysToShip: () => {

    }
};