const _ = require('lodash');

const DAYS_OF_WEEK = ['Sun', 'Mon', 'Tues', 'Wed', 'Thur', 'Fri', 'Sat'];

exports.config = {
  rest: {
    contextFilename: './context/earthquakeContext.json',
    sleep: 300000,
    next: function(prevContext) {
      //https://earthquake.usgs.gov/fdsnws/event/1/
      let url = 'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson'
      if (prevContext && prevContext.time) {
        const now = new Date();
        if (prevContext.time.min.substring(0,10) === now.toISOString().substring(0,10)) {
          const prevDay = new Date(prevContext.time.max);
          const startDay = new Date(prevDay.getTime() + 1000);
          url += `&starttime=${startDay.toISOString()}`;
        } else {
          const prevDay = new Date(prevContext.time.min.substring(0,10));
          const startDay = new Date(prevDay.getTime() + (24 * 60 * 60 * 1000));
          const endDay = new Date(prevDay.getTime() + (2 * 24 * 60 * 60 * 1000));
          url += `&starttime=${startDay.toISOString().substring(0,10)}&endtime=${endDay.toISOString().substring(0,10)}`;
        }
      } else {
        url += '&starttime=2013-01-01&endtime=2013-01-02';
      }
      return url;
    },
    watchlist: ['time']
  },
  es: {
    host: 'http://localhost:9200',
    index: 'earthquake',
    type: 'event'
  },
  extractRows: function(respBody) {
    return respBody.features;
  },
  transform: function(doc, callback) {
    doc.location = {
      lat: doc.geometry.coordinates[1],
      lon: doc.geometry.coordinates[0]
    };
    doc.magnitude = doc.properties.mag;
    doc.place = doc.properties.place;
    const date = new Date(doc.properties.time);
    doc.dayOfWeek = DAYS_OF_WEEK[date.getUTCDay()];
    doc.hourOfDay_str = "" + date.getUTCHours();
    doc.hourOfDay_num = date.getUTCHours();
    doc.time = date.toISOString();
    doc.updated = (new Date(doc.properties.updated)).toISOString();
    doc.url = doc.properties.url;
    doc.significance = doc.properties.sig;
    doc.tags = doc.properties.types.split(',');
    doc.title = doc.properties.title;
    doc._id = doc.id;

    delete doc.geometry;
    delete doc.id;
    delete doc.properties;
    delete doc.type;

    callback(doc);
  }
};
