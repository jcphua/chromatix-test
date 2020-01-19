const fs = require('fs'),
      path = require('path'),
      csvParser = require('csv-parser'),
      readline = require('readline');

const csv_filenames = ['test500', 'test4000', 'node-data-processing-medium-data'],
      csv_filename = csv_filenames[0],
      csv_path = path.join(__dirname, `../data/${ csv_filename }.csv`);

const __indexes = { _indexes: [] },
      __totals = { Total: { Revenue: 0, Cost: 0, Profit: 0 } },
      __dates = { 'Year': {}, 'YearMonth': {} },
      _re_dateComp = /^(\d{1,2})\/(\d{2})\/(\d{4})$/;

let src_data = [],
    _data = {},
    _regions = {},
    _dates = { ...__dates },
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
        src_data.push(row);

        const region = row['Region'];
        if (!_regions[region]) { 
            _regions[region] = { ...__totals, Countries: {}, _indexes: [] }; 
        }
        _regions[region]._indexes.push(row_ctr);

        if (!region_names.includes(region)) { region_names.push(region); }
        
        const order_date = row['Order Date'],
              _mt_dateComp = _re_dateComp.exec(order_date),
              order_year = _mt_dateComp[3],
              order_yearMonth = `${ _mt_dateComp[3] }-${ _mt_dateComp[1].padStart(2, '0') }`;
        if (!_dates['Year'][order_year]) { _dates['Year'][order_year] = { _indexes: [] }; }
        _dates['Year'][order_year]._indexes.push(row_ctr);

        if (!_dates['YearMonth'][order_yearMonth]) { _dates['YearMonth'][order_yearMonth] = { _indexes: [] }; }
        _dates['YearMonth'][order_yearMonth]._indexes.push(row_ctr);

        // if (row_ctr < 50) {
        //         console.log(order_date, _mt_dateComp, order_yearMonth);
        // }

        row_ctr++;
    })
    .on('end', () => {

        // console.log(_region_names.sort());
        // console.log(_regions);
        // console.log(_dates);

        console.timeEnd('readstream');
        console.log(`${ src_data.length } records processed.\n`);

        _fns_process.task1_region(src_data, _regions);

        _fns_process.task2_orderPriorities(src_data, _dates);
    });

const _fns_process = {
    _writeData: (_data, dest_filename) => {
        const output_filepath = path.join(__dirname, `../output/${ dest_filename }.json`);
        console.time(`Write file`);
        console.log(`\nWriting '${ dest_filename }' to:\n'${ output_filepath }`);
        fs.writeFileSync(output_filepath, JSON.stringify(_data));
        console.timeEnd(`Write file`);
    },
    task1_region: (src_data, _regions) => {
        let _data = {};
        _regions = _fns_genData.region_revenueCostProfit(src_data, _regions);
        _data['Regions'] = _regions;
        _data = _fns_genData.region_revenueCostProfit_itemTypes(src_data, _regions);

        _fns_process._writeData(_data, 'task1_region');
    },
    task2_orderPriorities: (src_data, _dates) => {
        let _data = {};
        _data = _fns_genData.date_orderPriorities(src_data, _dates);
        _fns_process._writeData(_data, 'task2_orderPriorities');
    },
    task3_avgTimeToShip: (_data, dest_filename) => {
        _fns_process._writeData(_data, 'task3_');
    }
};

const _fns_genData = {
    region_revenueCostProfit: (_data, _regions) => {
        console.time('All region Totals');
        for (let region in _regions) {
        // for (let i=0; i<Object.keys(_regions).length; i++) {
        //     let region = _regions[i] || Object.keys(_regions)[i];
            console.time(`Region: ${region}`);
            let _region = _regions[region];
            for (let idx = 0; idx < _region._indexes.length; idx++) {
                let row_index = _region._indexes[idx],
                    _row = _data[row_index],
                    country_name = _row['Country'];

                if (!_region['Countries'][country_name]) { 
                    _region['Countries'][country_name] = { 
                        'Total': {
                            'Revenue': 0,
                            'Cost': 0,
                            'Profit': 0
                        },
                        'ItemTypes': {},
                        _indexes: []
                    };
                }
                _region['Countries'][country_name]._indexes.push(row_index);
                
                _region['Countries'][country_name]['Total']['Revenue'] += parseFloat(_row['Total Revenue']);
                _region['Countries'][country_name]['Total']['Cost'] += parseFloat(_row['Total Cost']);
                _region['Countries'][country_name]['Total']['Profit'] += parseFloat(_row['Total Profit']);

                _region['Total']['Revenue'] += parseFloat(_row['Total Revenue']);
                _region['Total']['Cost'] += parseFloat(_row['Total Cost']);
                _region['Total']['Profit'] += parseFloat(_row['Total Profit']);
            }

            console.timeEnd(`Region: ${region}`);
        }
        console.timeEnd('All region Totals');
        return _regions;
    },
    region_revenueCostProfit_itemTypes: (_data, _regions) => {
        console.time('All region ItemTypes');
        let _item_types = {};
        for (let region in _regions) {
        // for (let i=0; i<Object.keys(_regions).length; i++) {
        //     let region = _regions[i] || Object.keys(_regions)[i];
            console.time(`Region: ${region}`);
            let _region = _regions[region];
            for (let country in _region['Countries']) {
                let _country = _region['Countries'][country];

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
        console.timeEnd('All region ItemTypes');
        return {
            'Regions': _regions,
            'Item Types': _item_types
        };
    },
    date_orderPriorities: (_data, _dates) => {
        console.time('\nOrder priorities');
        const _years = Object.keys(_dates['Year']).sort();
        for (let idx in _years) {
            const year = _years[idx];
            console.time(`Year ${year}`);
            let month = 0;
            do {
                month++;
                const yearMonth = `${ year }-${ month.toString().padStart(2, '0') }`;
                if (!_dates['YearMonth'][yearMonth]) { continue; }
                for (let idx = 0; idx < _dates['YearMonth'][yearMonth]._indexes.length; idx++) {
                    const row = _data[_dates['YearMonth'][yearMonth]._indexes[idx]],
                          order_priority = row['Order Priority'];
                    
                    if (!_dates['Year'][year.toString()][month.toString()]) { _dates['Year'][year.toString()][month.toString()] = {}; }
                    if (!_dates['Year'][year.toString()][month.toString()][order_priority]) { _dates['Year'][year.toString()][month.toString()][order_priority] = 0; }
                    _dates['Year'][year.toString()][month.toString()][order_priority] += 1;
                }
            } while (month < 12);

            console.timeEnd(`Year ${year}`);
        }
        console.timeEnd('\nOrder priorities');
        return _dates['Year'];
    },
    date_avgDaysToShip: () => {

    }
};