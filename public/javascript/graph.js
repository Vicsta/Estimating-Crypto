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

    let drawLine = function (pair, props, checkbox) {
        props = props || {};
        props.color = props.color || "steelblue";
        d3.csv('/data.csv?pair=' + pair, function (d) {
            d.date = parseTime(d.timestamp);
            d.price = +d.price;
            return d;
        }, function (error, data) {
            if (error) throw error;

            x.domain(d3.extent(data, function (d) {
                return d.date;
            }));
            y.domain(d3.extent(data, function (d) {
                return d.price;
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

            g.append("path")
                .datum(data)
                .attr("fill", "none")
                .attr("stroke", props.color)
                .attr("stroke-linejoin", "round")
                .attr("stroke-linecap", "round")
                .attr("stroke-width", 1.5)
                .attr("d", line);

            let parent = document.getElementById("graphLegend");
            let p = document.createElement("p");
            p.className = pair;
            p.innerHTML = "&nbsp;&nbsp;&nbsp;" + pair;
            let span = document.createElement("span");
            span.style.width = "18px";
            span.style.height = "18px";
            span.style.float = "left";
            span.style.background = props.color;
            p.appendChild(span);
            parent.appendChild(p);

            checkbox.disabled = false;

        });
    };

    function toggleLine(line, state, color, checkbox) {
        let lines = document.getElementById("lines");
        if (state === false) {
            // first two are G tags
            for (let i = 2; i < lines.children.length; i++) {
                if (lines.children[i].getAttribute("stroke") === color) {
                    lines.removeChild(lines.children[i]);
                    i--;
                    let legend = document.getElementById("graphLegend");
                    for (let x = 0; x < legend.children.length; x++) {
                        if (legend.children[x].className === line) {
                            legend.removeChild(legend.children[x]);
                            x--;
                        }
                    }
                }
            }
            checkbox.disabled = false;
        } else {
            drawLine(line, {color: color}, checkbox);
        }
    }

    let colors = {
      "eth" : "steelblue",
      "ltc" : "orange",
      "xbt" : "green",
      "xrp" : "red"
    };

    let checkBoxes = document.getElementById("graphSelect").getElementsByTagName("input");
    for(let key in colors) {
        for (let i = 0; i < checkBoxes.length; i++) {
            if (checkBoxes[i].name === key) {
                checkBoxes[i].onclick = function () {
                    this.disabled = true;
                    toggleLine(this.name, this.checked, colors[key], this);
                };
                drawLine(key, {color: colors[key]}, this);
            }
        }
    }


    //TODO axis values are not good after multiple drawLine calls -> Fixed: however not sure the scale will be the best fit (currently scales based on first line drawn)


});