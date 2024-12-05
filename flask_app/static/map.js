var usMap = L.map('usMap').setView([37.8, -96], 4);
var caMap = L.map('caMap').setView([67.8, -96], 3);
var worldMap = L.map('worldMap').setView([0, 0], 2);

var tiles = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
}).addTo(usMap);

var tiles = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
}).addTo(caMap);

var tiles = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
}).addTo(worldMap);

function getColor(d) {
    return d < -0.9   ? '#800026' :
           d < -0.5   ? '#BD0026' :
           d < -.2    ? '#E31A1C' :
           d < -.1    ? '#FC4E2A' :
           d < .1     ? '#FD8D3C' :
           d < .2     ? '#FEB24C' :
           d < .6     ? '#FED976' :
                      '#FFEDA0';
}

function style(feature) {
    return {
        fillColor: getColor(feature.properties.meanSentiment),
        weight: 2,
        opacity: 1,
        color: 'white',
        dashArray: '3',
        fillOpacity: 0.7
    };
}

L.geoJson(geoUS, {style: style}).addTo(usMap);
L.geoJson(geoCA, {style: style}).addTo(caMap);
L.geoJson(geoWORLD, {style: style}).addTo(worldMap);