const fs = require('fs'),
      path = require('path'),
      csvParser = require('csv-parser');

const csv_filenames = ['test500', 'test4000', 'node-data-processing-medium-data'],
      csv_filename = csv_filenames[2],
      csv_path = path.join(__dirname, `../data/${ csv_filename }.csv`);

const __indexes = { _indexes: [] },
      __totals = { Total: { Revenue: 0, Cost: 0, Profit: 0 } },
      __dates = { 'Year': {}, 'YearMonth': {} },
      __avgShipOrders = { 'AvgDaysToShip': 0, 'NumberOfOrders': 0 },
      _re_dateComp = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/;

let csv_data = [],
    _regions = {},
    _dates = { ...__dates },
    row_ctr = 0;

console.log('***********************************\n**** NODE DATA PROCESSING TEST ****\n***********************************');

console.time('Complete all processing');
console.log(`Reading data source:\n'${ csv_path }'`);
console.time('Read CSV');
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
            _regions[region] = { Total: { Revenue: 0, Cost: 0, Profit: 0 }, Countries: {}, _indexes: [] }; 
        }
        _regions[region]._indexes.push(row_ctr);

        // Init 'caching' index of data row for order date properties
        const order_date = row['Order Date'],
              _mtch_orderDate = _re_dateComp.exec(order_date),
              order_year = _mtch_orderDate[3],
              order_yearMonth_string = `${ _mtch_orderDate[3] }-${ _mtch_orderDate[1].padStart(2, '0') }`;
        if (!_dates['Year'][order_year]) { _dates['Year'][order_year] = { _indexes: [] }; }
        _dates['Year'][order_year]._indexes.push(row_ctr);

        if (!_dates['YearMonth'][order_yearMonth_string]) { 
            _dates['YearMonth'][order_yearMonth_string] = { 
                _indexes: [], 
                "_days_OrderedShippedDiff": [] 
            }; 
        }
        _dates['YearMonth'][order_yearMonth_string]._indexes.push(row_ctr);

        // Init average order/ship date difference calculations
        const ship_date = row['Ship Date'],
              _mtch_shipDate = _re_dateComp.exec(ship_date),
              _dt_orderDate = new Date(`${ order_yearMonth_string }-${ _mtch_orderDate[2].padStart(2, '0') }`),
              _dt_shipDate = new Date(`${ _mtch_shipDate[3] }-${ _mtch_shipDate[1].padStart(2, '0') }-${ _mtch_shipDate[2].padStart(2, '0') }`),
              order_ship_diff = (_dt_shipDate.getTime() - _dt_orderDate.getTime()) / 86400000;
        _dates['YearMonth'][order_yearMonth_string]['_days_OrderedShippedDiff'].push(order_ship_diff);

        if (row_ctr < 50) {
            // console.log(row_ctr, _dt_orderDate, _dt_shipDate, order_ship_diff);
        }

        row_ctr++;
    })
    .on('end', () => {

        _fns_process.emptyDir_output();

        console.log(`${ csv_data.length } records processed.\n`);
        console.timeEnd('Read CSV');

        _fns_process.task1_region(csv_data, _regions);
        _fns_process.task2_orderPriorities(csv_data, _dates);
        _fns_process.task3_avgTimeToShip(csv_data, _dates, _regions);

        console.timeEnd('Complete all processing');
    });

const _fns_process = {
    _emptyDir: (dest_path) => {
        if (!fs.existsSync(dest_path)) { return; }
        fs.readdirSync(dest_path).forEach((file, idx) => {
            let filepath = path.join(dest_path, file);
            fs.unlinkSync(filepath);
        });
    },
    _writeData: (_data, dest_filename) => {
        console.time(`Write file`);
        const output_filepath = path.join(__dirname, `../output/${ dest_filename }.json`);
        console.log(`\nWriting '${ dest_filename }' data to:\n'${ output_filepath }`);
        fs.writeFileSync(output_filepath, JSON.stringify(_data));
        console.timeEnd(`Write file`);
    },
    emptyDir_output: () => {
        const output_path = path.join(__dirname, `../output`);
        _fns_process._emptyDir(output_path);
    },
    task1_region: (_src_data, _regions) => {
        console.log('\n---------------');
        console.log('\n** [TASK 1] Generating Region and Country costing data **');
        console.time(`Complete (Regions)`);
        let _data = {};

        // _fns_process._writeData(_dates, '_RAW_task1_dates');

        _regions = _fns_genData.region_revenueCostProfit(_src_data, _regions);
        _data['Regions'] = _regions;
        _data = _fns_genData.region_revenueCostProfit_itemTypes(_src_data, _regions);

        console.timeEnd(`Complete (Regions)`);
        _fns_process._writeData(_data, 'task1_region');
    },
    task2_orderPriorities: (_src_data, _dates) => {
        console.log('\n---------------');        
        console.log('\n** [TASK 2] Generating Order Priority data **');
        console.time(`Complete (Order priorities)`);
        let _data = {};

        // _fns_process._writeData(_dates, '_RAW_task2_dates');

        _data = _fns_genData.date_orderPriorities(_src_data, _dates);
        console.timeEnd(`Complete (Order priorities)`);
        _fns_process._writeData(_data, 'task2_orderPriorities');
    },
    task3_avgTimeToShip: (_src_data, _dates, _regions) => {
        console.log('\n---------------');        
        console.log('\n** [TASK 3] Generating Average Days To Ship data **');
        console.time(`Complete (Average time to ship)`);
        let _data = {}, data2 = {};

        // _fns_process._writeData(_dates, '_RAW_task3_dates');

        _data = _fns_genData.date_avgDaysToShip(_src_data, _dates, _regions);
        // _fns_process._writeData(_data, '_RAW_task3_data');
        _data2 = _fns_genData.date_avgDaysToShip_Region(_src_data, _dates, _regions, _data);
        console.timeEnd(`Complete (Average time to ship)`);
        _fns_process._writeData(_data2, 'task3_avgDaysToShip');
    }
};

const _fns_genData = {
    _arr_avg: (arr) => arr.reduce((a, b) => a + b) / arr.length,
    region_revenueCostProfit: (_src_data, _regions) => {
        console.time('a) Region Totals');

        for (let region in _regions) {
            // console.time(`Region: ${region}`);
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

            // console.timeEnd(`Region: ${region}`);
        }
        console.timeEnd('a) Region Totals');
        return _regions;
    },
    region_revenueCostProfit_itemTypes: (_src_data, _regions) => {
        console.time('b) Region ItemTypes');

        let _data = {},
            _item_types = {};
        for (let region in _regions) {
            // console.time(`Region: ${region}`);
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

            // console.timeEnd(`Region: ${region}`);
        }
        _data = {
            'Regions': _regions,
            'Item Types': _item_types
        };
        console.timeEnd('b) Region ItemTypes');
        return _data;
    },
    date_orderPriorities: (_src_data, _dates) => {
        console.time('Order priorities');
        
        const _years = Object.keys(_dates['Year']).sort();
        let _data = {};
        for (let yr in _years) {
            const year = _years[yr];
            // console.time(`Year ${year}`);
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
                    
                    if (!_data[year][month_string]) { _data[year][month_string] = {}; }
                    if (!_data[year][month_string][order_priority]) { _data[year][month_string][order_priority] = 0; }
                    _data[year][month_string][order_priority] += 1;
                }
            } while (month < 12);

            // console.timeEnd(`Year ${year}`);
        }
        console.timeEnd('Order priorities');
        return _data;
    },
    date_avgDaysToShip: (_src_data, _dates, _regions) => {
        console.time('a) Average Days To Ship');

        const _years = Object.keys(_dates['Year']).sort();
        let _data = {};
        for (let yr in _years) {
            const year = _years[yr];
            // console.time(`Year ${year}`);
            if (!_data[year]) { _data[year] = {}; }
            let month = 0;
            do {
                month++;
                const month_string = month.toString().padStart(2, '0'),
                      yearMonth_string = `${ year }-${ month_string }`;
                if (!_dates['YearMonth'][yearMonth_string]) { continue; }
                for (let idx = 0; idx < _dates['YearMonth'][yearMonth_string]._indexes.length; idx++) {
                    const row = _src_data[_dates['YearMonth'][yearMonth_string]._indexes[idx]];
                    // console.log(yearMonth_string, _dates['YearMonth'][yearMonth_string]._indexes[idx], row);

                    if (!_data[year][month_string]) { _data[year][month_string] = { ...__avgShipOrders, ..._dates['YearMonth'][yearMonth_string] }; }
                    
                    _data[year][month_string]['AvgDaysToShip'] = _fns_genData._arr_avg(_dates['YearMonth'][yearMonth_string]['_days_OrderedShippedDiff']);
                    _data[year][month_string]['NumberOfOrders'] += 1;
                }
            } while (month < 12);

            // console.timeEnd(`Year ${year}`);
        }
        console.timeEnd('a) Average Days To Ship');
        return _data;
    },
    date_avgDaysToShip_Region: (_src_data, _dates, _regions, _data) => {
        console.time('b) Average Days To Ship - Regions');
        
        const _years = Object.keys(_dates['Year']).sort();
        // let _data = {};
        for (let yr in _years) {
            const year = _years[yr];
            // console.time(`Year ${year}`);
            if (!_data[year]) { _data[year] = {}; }
            let month = 0;
            do {
                month++;
                const month_string = month.toString().padStart(2, '0'),
                      yearMonth_string = `${ year }-${ month_string }`;
                if (!_dates['YearMonth'][yearMonth_string]) { continue; }
                if (!_data[year][month_string]) { _data[year][month_string] = { 'Regions': {} }; }
                for (let idx = 0; idx < _dates['YearMonth'][yearMonth_string]._indexes.length; idx++) {
                    const row_index = _dates['YearMonth'][yearMonth_string]._indexes[idx],
                          row = _src_data[row_index],
                          region = row['Region'],
                          country = row['Country'];
                    
                    if (!_data[year][month_string]['Regions']) { _data[year][month_string]['Regions'] = {}; }
                    if (!_data[year][month_string]['Regions'][region]) { _data[year][month_string]['Regions'][region] = { ...__avgShipOrders, 'Countries': {}, _indexes: [], '_days_OrderedShippedDiff': [] }; }
                    if (!_data[year][month_string]['Regions'][region]['Countries']) { _data[year][month_string]['Regions'][region]['Countries'] = {}; }
                    if (!_data[year][month_string]['Regions'][region]['Countries'][country]) { _data[year][month_string]['Regions'][region]['Countries'][country] = { ...__avgShipOrders, _indexes: [], '_days_OrderedShippedDiff': [] }; }

                    _data[year][month_string]['Regions'][region]._indexes.push(row_index);
                    _data[year][month_string]['Regions'][region]['Countries'][country]._indexes.push(row_index);
                    
                    _data[year][month_string]['Regions'][region]['NumberOfOrders'] += 1;
                    _data[year][month_string]['Regions'][region]['Countries'][country]['NumberOfOrders'] += 1;

                    const order_date = row['Order Date'],
                    ship_date = row['Ship Date'],
                    _mtch_orderDate = _re_dateComp.exec(order_date),
                    _mtch_shipDate = _re_dateComp.exec(ship_date),
                    _dt_orderDate = new Date(`${ yearMonth_string }-${ _mtch_orderDate[2].padStart(2, '0') }`),
                    _dt_shipDate = new Date(`${ _mtch_shipDate[3] }-${ _mtch_shipDate[1].padStart(2, '0') }-${ _mtch_shipDate[2].padStart(2, '0') }`),
                    order_ship_diff = (_dt_shipDate.getTime() - _dt_orderDate.getTime()) / 86400000;

                    _data[year][month_string]['Regions'][region]['AvgDaysToShip'] = order_ship_diff;
                    _data[year][month_string]['Regions'][region]['Countries'][country]['AvgDaysToShip'] = order_ship_diff;

                    _data[year][month_string]['Regions'][region]['_days_OrderedShippedDiff'].push(order_ship_diff);
                    _data[year][month_string]['Regions'][region]['Countries'][country]['_days_OrderedShippedDiff'].push(order_ship_diff);
                    if (_data[year][month_string]['Regions'][region]['Countries'][country]._indexes.length > 1) {
                        _data[year][month_string]['Regions'][region]['Countries'][country]['AvgDaysToShip'] = _fns_genData._arr_avg(_data[year][month_string]['Regions'][region]['Countries'][country]['_days_OrderedShippedDiff']);
                    }
                    if (idx === _dates['YearMonth'][yearMonth_string]._indexes.length - 1) {
                        if (_data[year][month_string]['Regions'][region]['Countries'][country]._indexes.length > 1) {
                            _data[year][month_string]['Regions'][region]['Countries'][country]['AvgDaysToShip'] = _fns_genData._arr_avg(_data[year][month_string]['Regions'][region]['Countries'][country]['_days_OrderedShippedDiff']);
                        }
                        else {
                            _data[year][month_string]['Regions'][region]['Countries'][country]['AvgDaysToShip'] = order_ship_diff;
                        }
                        if (_data[year][month_string]['Regions'][region]._indexes.length > 1) {
                            _data[year][month_string]['Regions'][region]['AvgDaysToShip'] = _fns_genData._arr_avg(_data[year][month_string]['Regions'][region]['_days_OrderedShippedDiff']);
                        }
                        else {
                            _data[year][month_string]['Regions'][region]['AvgDaysToShip'] = order_ship_diff;
                        }
                    }

                }
            } while (month < 12);

            // console.timeEnd(`Year ${year}`);
        }
        console.timeEnd('b) Average Days To Ship - Regions');
        return _data;
    }
};