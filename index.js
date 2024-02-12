const {Storage} = require('@google-cloud/storage');
const csv = require('csv-parser');
const {BigQuery} = require('@google-cloud/bigquery');

const bq = new BigQuery();
const datasetId = 'weather_etl';
const tableId = 'weather';

exports.readObservation = (file, context) => {

    const gcs = new Storage();

    const dataFile = gcs.bucket(file.bucket).file(file.name);

    var newContent = [];

    dataFile.createReadStream()
    .on('error', () => {
        // Handle the error
        console.error(error);
    })
    .pipe(csv())
    .on('data', (field) => {
        // Log row data
        newContent = transformFields(field);
        console.log(newContent);
        writeToBq(newContent);
    })
    .on('end', () => {
        // Handle the CSV
        console.log('End!');
    });
    
    

}

function transformFields(field) {
    Object.keys(field).map( (key) => {
        console.log(`Key: ${key} Value: ${field[key]}`);
    });

    field.station = '724380-93819';

    field.year = parseInt(field.year);
    field.month = parseInt(field.month);
    field.day = parseInt(field.day);
    field.hour = parseInt(field.hour);
    field.winddirection = parseInt(field.winddirection);

    if (field.sky == -9999) {
        field.sky = null;
    } else{
        field.sky = parseInt(field.sky);
    }

    field.airtemp = parseFloat(field.airtemp/10);
    field.dewpoint = parseFloat(field.dewpoint/10);
    field.pressure = parseFloat(field.pressure/10);
    field.windspeed = parseFloat(field.windspeed/10);

    if (field.precip1hour == -9999) {
        field.precip1hour = null;
    } else{
        field.precip1hour = parseFloat(field.precip1hour/10);
    }

    if (field.precip6hour == -9999) {
        field.precip6hour = null;
    } else{
        field.precip6hour = parseFloat(field.precip6hour/10);
    }
    return field;

};


// HELPER FUNCTION

async function writeToBq(field) {
    // BQ expects an array of objects, but this function only recieves 1
    var rows = []; // Empty Array
    rows.push(field);

    await bq
    .dataset(datasetId)
    .table(tableId)
    .insert(rows)
    .then( () => {
        rows.forEach ( (row) => { console.log(`Inserted: ${row}`) } )
    } )
    .catch( (err) => { console.error(`ERROR: ${err}`) } )
}


