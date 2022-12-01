var fs = require('fs');
var et = require('elementtree');
var _ = require('lodash');
var app = require('express')();
var http = require('http').Server(app);

const fetch = require('node-fetch');

var port = process.env.PORT || 4001;

var cache = {};

var queryBase = "https://opendata.fmi.fi/wfs?request=getFeature&" +
  "storedquery_id=fmi::observations::weather::multipointcoverage&" +
  "parameters=ws_10min,wg_10min,wd_10min,t2m,rh,ri_10min,vis,n_man&";

var stationCoordinatePath = "wfs:member/omso:GridSeriesObservation/om:featureOfInterest/sams:SF_SpatialSamplingFeature/sams:shape/gml:MultiPoint/gml:pointMember/gml:Point";  // "/gml:pos";
var positionsPath = "wfs:member/omso:GridSeriesObservation/om:result/gmlcov:MultiPointCoverage/gml:domainSet/gmlcov:SimpleMultiPoint/gmlcov:positions";
var observationsPath = "wfs:member/omso:GridSeriesObservation/om:result/gmlcov:MultiPointCoverage/gml:rangeSet/gml:DataBlock/gml:doubleOrNilReasonTupleList";

function addStationMetadata(stationValues, observation) {
  observation.coordString = observation.lat + "," + observation.long;
  observation.stationName = stationValues[observation.coordString] || "";
  return observation;
}

function filterNaNValues(observation) {
  return _.pickBy(observation, function(value, key) {
    return String(value) != "NaN";
  });
}

function hasNonNaNMeasurements(observation) {
  return _.chain(observation)
    .keys()
    .difference(['lat', 'long', 'time'])
    .size()
    .value() != 0
}

function parseObservationBody(body) {
  var etree = et.parse(body);

  var stations = etree.findall(stationCoordinatePath);
  var positions = etree.findall(positionsPath);
  var observations = etree.findall(observationsPath);

  if (!positions || !positions.length ||
      !observations || !observations.length) {
    return {}
  }

  var stationValues = parsePositions(stations);
  var positionTimeValues = parsePositionTimeString(positions[0].text);
  var observationValues = parseObservationString(observations[0].text);

  var combined = _.chain([])
    .merge(positionTimeValues, observationValues)
    .map(filterNaNValues)
    .filter(hasNonNaNMeasurements)
    .map(function(observation) {
      return addStationMetadata(stationValues, observation);
    })
    .groupBy(function(observation) {
      return observation.coordString;
    })
    .mapValues(function(array) { return _.orderBy(array, 'time', 'desc'); })
    .value();

  // console.log("Combined: " + JSON.stringify(combined, null, 2));

    return { observations: combined }
}

function parsePositions(positions) {
  var values = positions.reduce(function(result, pos) {
    var coordinates = (pos.find("gml:pos").text || "").trim().replace(/ /, ",");
    var name = (pos.find("gml:name").text || "").trim();
    result[coordinates] = name;
    return result;
  }, {});

  return values || {}
}

function parsePositionTimeString(positionTimeRows) {
  return mapToObject(splitRows(positionTimeRows), [ 'lat', 'long', 'time'] );
}

function parseObservationString(observationRows) {
  return mapToObject(splitRows(observationRows),
                     [ 'windSpeed',
                       'windSpeedGust',
                       'windDirection',
                       'airTemperature',
                       'relativeHumidity',
                       'precipitationAmount',
                       'visibility',
                       'amountOfCloud' ]);
}

function mapToObject(rows, expectedFields) {
  return _.chain(rows)
    .map(function(s) { return s.trim(); })
    .map(function(s) { return s.split(/\s+/); })
    .filter(function(values) { if (!expectedFields) return true;
                               else return expectedFields.length === values.length; })
    .map(function(values) { if (!expectedFields) return values;
                            else return _.zipObject(expectedFields, values); })
    .value();
}

function splitRows(stringWithRows) {
    return stringWithRows.match(/[^\r\n]+/g);
}

const MINUTE = 60000;

function setupQuery(base,
                    lat1, lon1,
                    lat2, lon2) {
  const timeNow = Date.now();
  var urlData = { };
  urlData.boundingBox = [lon1, lat1, lon2, lat2].join(",");
  urlData.url = base +
    "bbox=" + urlData.boundingBox + "&" +
    "timestep=10&" +
    "starttime=" + formatTime(timeNow - 20 * MINUTE)  + "&" +
    "endtime=" + formatTime(timeNow);
  return urlData;
}

function formatTime(time) {
  var m = new Date(time).toISOString().match(/(.+)\.\d{3}(Z)/);
  return m ? m[1] + m[2] : "";
}

function isGoodCoordinate(coordinate) {
  if (coordinate &&
      String(coordinate).match(/^\d{1,3}(\.\d{1,7})?$/)) {
    var numCoordinate = parseFloat(coordinate);
    if (numCoordinate > 19 && numCoordinate < 72) {
      return true;
    }
  }
  return false;
}

app.get("/1/observations", async (req, res) => {
  var query = req.query;
  if ( ![ query.lat1, query.lat2, query.lon1, query.lon2].every(isGoodCoordinate)) {
    res.status(400).end();
    return;
  }

  var urlData = setupQuery(queryBase,
                           query.lat1, query.lon1,
                           query.lat2, query.lon2);

  const cachedData = cache[urlData.boundingBox];
  if (cachedData &&
      cachedData.date > Date.now() - 10 * MINUTE) {
    // Found in cache.
    console.log("Replying with cached data.");
    res.send(cachedData.data);
    return;
  }

  console.log("Sending request to ", urlData.url);

  try {
    const fetchRequest = await fetch(urlData.url)
    const body = await fetchRequest.text();

    var parsedData = parseObservationBody(body);

    cache[urlData.boundingBox] = {
      date: Date.now(),
      data: parsedData
    };

    res.send(parsedData);
  } catch (error) {
    console.log("Request to FMI failed", error);
    res.status(400).end()
  }
});

http.listen(port);
