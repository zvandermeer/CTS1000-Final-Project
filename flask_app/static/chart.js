const data = {
    labels: Object.keys(yearData),
    datasets: [{
        label: "Data",
        data: Object.values(yearData),
        fill:false,
        borderColor: 'rgb(75,192,192)',
        tension: 0.1
    }]
};

const config = {
    type: 'line',
    data: data,
};

console.log(yearData)
console.log(Object.keys(yearData))
console.log(Object.values(yearData))

const ctx = document.getElementById('chart');

new Chart(ctx, config);