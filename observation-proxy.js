var fs = require('fs');
var et = require('elementtree');
var _ = require('lodash');
var app = require('express')();
var http = require('http').Server(app);
var request = require('request');

var port = process.env.PORT || 4001;

var cache = {};

var queryBase = "http://opendata.fmi.fi/wfs?request=getFeature&" +
  "storedquery_id=fmi::observations::weather::multipointcoverage&" +
  "parameters=ws_10min,wg_10min,wd_10min,t2m,rh,r_1h,vis,n_man&";

var stationCoordinatePath = "wfs:member/omso:GridSeriesObservation/om:featureOfInterest/sams:SF_SpatialSamplingFeature/sams:shape/gml:MultiPoint/gml:pointMember/gml:Point";  // "/gml:pos";
var positionsPath = "wfs:member/omso:GridSeriesObservation/om:result/gmlcov:MultiPointCoverage/gml:domainSet/gmlcov:SimpleMultiPoint/gmlcov:positions";
var observationsPath = "wfs:member/omso:GridSeriesObservation/om:result/gmlcov:MultiPointCoverage/gml:rangeSet/gml:DataBlock/gml:doubleOrNilReasonTupleList";


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
    .map(function(observation) {
      observation.coordString = observation.lat + "," + observation.long;
      observation.stationName = stationValues[observation.coordString] || ""
      return observation;
    })
    .groupBy(function(observation) {
      return observation.coordString;
    })
    .mapValues(function(array) { return _.sortBy(array, 'time'); })
    .value();

  // console.log("Combined: " + JSON.stringify(combined, null, 2));

  return combined;
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
                        [ 'windSpeed', 'windSpeedGust', 'windDirection',
                          'airTemperature', 'relativeHumidity',
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

function setupQuery(base,
                    lat1, lon1,
                    lat2, lon2) {
  var timeNow = Date.now();
  var urlData = { };
  urlData.boundingBox = [lon1, lat1, lon2, lat2].join(",");
  urlData.url = base +
    "bbox=" + urlData.boundingBox + "&" +
    "timestep=10&" +
    "starttime=" + formatTime(timeNow - 2 * 3600000)  + "&" +
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
    if (numCoordinate > 20 && numCoordinate < 70) {
      return true;
    }
  }
  return false;
}

app.get("/1/observations", function(req, res) {
  var query = req.query;
  if ( ![ query.lat1, query.lat2, query.lon1, query.lon2].every(isGoodCoordinate)) {
    res.status(400).end();
    return;
  }

  var urlData = setupQuery(queryBase,
                           query.lat1, query.lon1,
                           query.lat2, query.lon2);

  var cachedData = cache[urlData.boundingBox];
  if (cachedData &&
      cachedData.date > Date.now() - 10 * 60000) {
    // Found in cache.
    console.log("Replying with cached data.");
    res.send(cachedData.data);
    return;
  }

  console.log("Sending request to ", urlData.url);

  request(urlData.url, function(error, response, body) {
    if (error || response.statusCode !== 200) {
      console.log("Request to FMI failed: ", error, response.statusCode, body);
      res.status(response.statusCode).end();
      return;
    }

    var parsedData = parseObservationBody(body);
    cache[urlData.boundingBox] = {
      date: Date.now(),
      data: parsedData
    };
    res.send(parsedData);
  });
});

http.listen(port);
