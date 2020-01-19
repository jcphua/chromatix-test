const fs = require('fs'),
      path = require('path'),
      csvParser = require('csv-parser');

const csv_filenames = ['test500', 'test4000', 'node-data-processing-medium-data'],
      csv_filename = csv_filenames[0],
      csv_path = path.join(__dirname, `../data/${ csv_filename }.csv`);

const __indexes = { _indexes: [] },
      __totals = { Total: { Revenue: 0, Cost: 0, Profit: 0 } },
      __dates = { 'Year': {}, 'YearMonth': {} },
      _re_dateComp = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/;

let csv_data = [],
    _regions = {},
    _dates = { ...__dates },
    row_ctr = 0;

console.log(`Data source:\n'${ csv_path }'`);

console.time('readstream');
fs.createReadStream(csv_path)
    .on('error', () => {
        console.error(arguments);
    })
    .pipe(csvParser())
    .on('data', (row) => {
        csv_data.push(row);

        // Init 'caching' index of data row for region properties
        const region = row['Region'];
        if (!_regions[region]) { 
            _regions[region] = { ...__totals, Countries: {}, ...__indexes }; 
        }
        _regions[region]._indexes.push(row_ctr);

        // Init 'caching' index of data row for order date properties
        const order_date = row['Order Date'];
        if (!_re_dateComp.test(order_date)) { console.log(row_ctr, row); }
        const _mtch_orderDate = _re_dateComp.exec(order_date),
              order_year = _mtch_orderDate[3],
              order_yearMonth_string = `${ _mtch_orderDate[3] }-${ _mtch_orderDate[1].padStart(2, '0') }`;
        if (!_dates['Year'][order_year]) { _dates['Year'][order_year] = { _indexes: [] }; }
        _dates['Year'][order_year]._indexes.push(row_ctr);

        if (!_dates['YearMonth'][order_yearMonth_string]) { 
            _dates['YearMonth'][order_yearMonth_string] = { 
                _indexes: [], 
                "days_OrderedShippedDiff": [] 
            }; 
        }
        _dates['YearMonth'][order_yearMonth_string]._indexes.push(row_ctr);

        // Init 
        const ship_date = row['Ship Date'],
              _mtch_shipDate = _re_dateComp.exec(ship_date),
              _dt_orderDate = new Date(`${ order_yearMonth_string }-${ _mtch_orderDate[2].padStart(2, '0') }`),
              _dt_shipDate = new Date(`${ _mtch_shipDate[3] }-${ _mtch_shipDate[1].padStart(2, '0') }-${ _mtch_shipDate[2].padStart(2, '0') }`),
              order_ship_diff = (_dt_shipDate.getTime() - _dt_orderDate.getTime()) / 86400000;
        _dates['YearMonth'][order_yearMonth_string]['days_OrderedShippedDiff'].push(order_ship_diff);

        if (row_ctr < 50) {
            console.log(row_ctr, _dt_orderDate, _dt_shipDate, order_ship_diff);
        }

        row_ctr++;
    })
    .on('end', () => {

        // console.log(_regions);
        // console.log(_dates);

        console.timeEnd('readstream');
        console.log(`${ csv_data.length } records processed.\n`);

        _fns_process.task1_region(csv_data, _regions);
        _fns_process.task2_orderPriorities(csv_data, _dates);
        _fns_process.task3_avgTimeToShip(csv_data, _dates, _regions);
    });

const _fns_process = {
    _writeData: (_data, dest_filename) => {
        console.time(`Write file`);
        const output_filepath = path.join(__dirname, `../output/${ dest_filename }.json`);
        console.log(`\nWriting '${ dest_filename }' to:\n'${ output_filepath }`);
        fs.writeFileSync(output_filepath, JSON.stringify(_data));
        console.timeEnd(`Write file`);
    },
    task1_region: (_src_data, _regions) => {
        console.log('\n---------------');
        console.time(`Task 1: Regions`);
        let _data = {};
        _regions = _fns_genData.region_revenueCostProfit(_src_data, _regions);
        _data['Regions'] = _regions;
        _data = _fns_genData.region_revenueCostProfit_itemTypes(_src_data, _regions);

        _fns_process._writeData(_data, 'task1_region');
        console.timeEnd(`Task 1: Regions`);
    },
    task2_orderPriorities: (_src_data, _dates) => {
        console.log('\n---------------');        
        console.time(`Task 2: Order priorities`);
        let _data = {};
        _data = _fns_genData.date_orderPriorities(_src_data, Object.assign({}, _data, _dates));
        _fns_process._writeData(_dates, '_dates');
        // _fns_process._writeData(_data['Year'], 'task2_orderPriorities');
        _fns_process._writeData(_data, 'task2_orderPriorities');
        console.timeEnd(`Task 2: Order priorities`);
    },
    task3_avgTimeToShip: (_src_data, _dates, _regions) => {
        console.log('\n---------------');        
        console.time(`Task 3: Average time to ship`);
        _data = _fns_genData.date_avgDaysToShip(_src_data, _dates, _regions);
        // _fns_process._writeData(_data, 'task3_avgDaysToShip');
        console.timeEnd(`Task 3: Average time to ship`);
    }
};

const _fns_genData = {
    region_revenueCostProfit: (_src_data, _regions) => {
        console.log('\n** Generating Region and Country costing data **');
        console.time('All region Totals');
        for (let region in _regions) {
        // for (let i=0; i<Object.keys(_regions).length; i++) {
        //     let region = _regions[i] || Object.keys(_regions)[i];
            console.time(`Region: ${region}`);
            let _region = _regions[region];
            for (let idx = 0; idx < _region._indexes.length; idx++) {
                let row_index = _region._indexes[idx],
                    _row = _src_data[row_index],
                    country_name = _row['Country'];

                if (!_region['Countries'][country_name]) { 
                    _region['Countries'][country_name] = { 
                        'Total': {
                            'Revenue': 0,
                            'Cost': 0,
                            'Profit': 0
                        }, // ...__totals,
                        'ItemTypes': {},
                        _indexes: [] // ...__indexes
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
    region_revenueCostProfit_itemTypes: (_src_data, _regions) => {
        console.log('\n** Generating Region, Country and ItemType data **');
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
                        _row = _src_data[row_index],
                        item_type = _row['Item Type'];

                    if (!_country['ItemTypes'][item_type]) { 
                        _country['ItemTypes'][item_type] = {
                            'Total': {
                                'Revenue': 0,
                                'Cost': 0,
                                'Profit': 0
                            }, // ...__totals,
                            _indexes: [] // ...__indexes
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
                            'Profit': 0, // ...__totals[Total]
                            _indexes: [] // ...__indexes
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
    date_orderPriorities: (_src_data, _dates) => {
        console.log('\n** Generating Order Priority data **');
        console.time('\nOrder priorities');
        let _data = {};
        const _years = Object.keys(_dates['Year']).sort();
        for (let yr in _years) {
            const year = _years[yr];
            console.time(`Year ${year}`);
            _data[year] = {};
            let month = 0;
            do {
                month++;
                const month_string = month.toString().padStart(2, '0'),
                      yearMonth_string = `${ year }-${ month_string }`;
                if (!_dates['YearMonth'][yearMonth_string]) { continue; }
                for (let idx = 0; idx < _dates['YearMonth'][yearMonth_string]._indexes.length; idx++) {
                    const row = _src_data[_dates['YearMonth'][yearMonth_string]._indexes[idx]],
                          order_priority = row['Order Priority'];
                    
                    if (!_data[year.toString()][month_string]) { _data[year.toString()][month_string] = {}; }
                    if (!_data[year.toString()][month_string][order_priority]) { _data[year.toString()][month_string][order_priority] = 0; }
                    _data[year.toString()][month_string][order_priority] += 1;
                }
            } while (month < 12);

            console.timeEnd(`Year ${year}`);
        }
        console.timeEnd('\nOrder priorities');
        // console.log(JSON.stringify(_dates));
        return _data;
    },
    date_avgDaysToShip: (_src_data, _dates, _regions) => {
        console.log('\n** Generating Average Days To Ship data **');
        console.time('\nAverage Days To Ship');

        const _years = Object.keys(_dates['Year']).sort();
        for (let yr in _years) {
            const year = _years[yr];
            console.time(`Year ${year}`);
            let month = 0;
            do {
                month++;
                const month_string = month.toString().padStart(2, '0'),
                      yearMonth_string = `${ year }-${ month_string }`;
                if (!_dates['YearMonth'][yearMonth_string]) { continue; }
                for (let idx = 0; idx < _dates['YearMonth'][yearMonth_string]._indexes.length; idx++) {
                    const row = _src_data[_dates['YearMonth'][yearMonth_string]._indexes[idx]];
                }
            } while (month < 12);

            console.timeEnd(`Year ${year}`);
        }
        console.timeEnd('\nAverage Days To Ship');
        return {

        };
    }
};