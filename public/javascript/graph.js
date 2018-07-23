window.addEventListener('load', function () {

    let svg = d3.select("svg"),
        margin = {top: 20, right: 20, bottom: 30, left: 50},
        width = +svg.attr("width") - margin.left - margin.right,
        height = +svg.attr("height") - margin.top - margin.bottom,
        g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")").attr("id", "lines");

    let parseTime = function (time) {
        timestamp = Math.trunc(parseFloat(time) * 1000);
        return new Date(timestamp)
    };

    let x = d3.scaleTime()
        .rangeRound([0, width]);

    let y = d3.scaleLinear()
        .rangeRound([height, 0]);

    let line = d3.line()
        .x(function (d) {
            return x(d.date);
        })
        .y(function (d) {
            return y(d.price);
        });

    let singletonX = null;
    let singletonY = null;

    let drawLine = function (pair, props) {
        props = props || {};
        props.color = props.color || "steelblue";
        d3.csv('/data.csv?pair=' + pair, function (d) {
            d.date = parseTime(d.timestamp);
            d.price = +d.price;
            d.predicted = +d.predicted;
            return d;
        }, function (error, data) {
            if (error) throw error;

            // Get the actual data, price and timestamp (converted to date from above)
            let actual = data.map(function(d) {
                return {
                    price: d.price,
                    date: d.date
                }
            });

            // Get the predicted data, predicted and timestamp (converted to date from above)
            let predicted = data.map(function(d) {
                return {
                    price: d.predicted,
                    date: d.date
                }
            });

            // Extend the Y domain to encompass all values in actual AND predicted
            y.domain(d3.extent([].concat(data.map(function(d) {
                return d.price;
            }), data.map(function(d) {
                return d.predicted;
                })
            )));

            x.domain(d3.extent(data, function (d) {
                return d.date;
            }));

            if (singletonX === null) {
                singletonX = 1;
                g.append("g")
                    .attr("transform", "translate(0," + height + ")")
                    .call(d3.axisBottom(x))
                    .select(".domain")
                    .remove();
            }

            if (singletonY === null) {
                singletonY = 1;
                g.append("g")
                    .call(d3.axisLeft(y))
                    .append("text")
                    .attr("fill", "#000")
                    .attr("transform", "rotate(-90)")
                    .attr("y", 6)
                    .attr("dy", "0.71em")
                    .attr("text-anchor", "end")
                    .text("Price ($)");
            }

            // Create area
            let area = d3.area()
                // .interpolate("cardinal")
                .x( function(d) { return x(d.date) } )
                // .x1( function(d) { return x(predicted.date) } )
                .y0( function(d) { return y(d.price) } )
                .y1( function(d) { return y(d.predicted) } );

            // Append area
            g.append('path')
                .datum(data)
                .attr('class', 'area')
                .attr('fill', 'lightsteelblue')
                .attr('d', area);

            // Append actual path
            g.append("path")
                .datum(actual)
                .attr("fill", "none")
                .attr("stroke", props.color)
                .attr("stroke-linejoin", "round")
                .attr("stroke-linecap", "round")
                .attr("stroke-width", 1.5)
                .attr("d", line);

            // Append predicted path
            g.append("path")
                .datum(predicted)
                .attr("fill", "none")
                .attr("stroke", "red")
                .attr("stroke-linejoin", "round")
                .attr("stroke-linecap", "round")
                .attr("stroke-width", 1.5)
                .attr("d", line);

            // Mean Error
            let ME = data.reduce((acc, d) => acc + (d.predicted - d.price), 0) / data.length;
            console.log(ME);

            let MSE = data.reduce((acc, d) => acc + Math.pow(d.predicted - d.price, 2), 0) / data.length;
            console.log(MSE);
            // Mean Squared Error

            let parent = document.getElementById("graphLegend");
            let p = document.createElement("p");
            p.className = pair;
            p.innerHTML = pair + " - MODEL1";
            parent.appendChild(p);

            // Describe the actual price line
            p = document.createElement("p");
            p.className = pair;
            p.innerHTML = "&nbsp;&nbsp;&nbsp;" + pair + " Actual Price";
            let span = document.createElement("span");
            span.style.width = "18px";
            span.style.height = "18px";
            span.style.float = "left";
            span.style.background = props.color;
            p.appendChild(span);
            parent.appendChild(p);

            // Describe the predicted line
            p = document.createElement("p");
            p.className = pair;
            p.innerHTML = "&nbsp;&nbsp;&nbsp;" + pair + " Predicted Price";
            span = document.createElement("span");
            span.style.width = "18px";
            span.style.height = "18px";
            span.style.float = "left";
            span.style.background = "red";
            p.appendChild(span);
            parent.appendChild(p);

            p = document.createElement("p");
            p.innerHTML = "Mean Error: " + ME;
            parent.appendChild(p);

            p = document.createElement("p");
            p.innerHTML = "Mean Squared Error: " + MSE;
            parent.appendChild(p);

            checkbox.disabled = false;

        });
    };

    let colors = {
        // "eth" : "steelblue",
        // "ltc" : "orange",
        // "xbt" : "green",
        // "xrp" : "red",
        "CSV1": "purple"
    };

    for(let key in colors) {
        drawLine(key, {color: colors[key]});
    }

});