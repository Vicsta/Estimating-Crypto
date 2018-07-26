window.addEventListener('load', function () {
    let min = 1.4951196E12;
    let max = 1.5305724E12;

    $('#range-start').text(new Date(min).toLocaleString())
    $('#range-end').text(new Date(max).toLocaleString())
    var slider = noUiSlider.create(document.getElementById('slider'), {
    	start: [ min, max ],
    	range: {
    		'min': [  min ],
    		'max': [ max ]
    	}
    });

    slider.on("change", function(){
      let range = slider.get()
      var min = new Date(Math.trunc(parseFloat(range[0])))
      var max = new Date(Math.trunc(parseFloat(range[1])))
      $('#range-start').text(new Date(min).toLocaleString())
      $('#range-end').text(new Date(max).toLocaleString())
      for (let key in colors) {
        $("#" + $("#select").val()).empty()
          drawLine(key, $("#select").val(), {color: colors[key]});
      }
    });


    let colors = {
        "XETHZUSD": "blue",
        "XLTCZUSD": "blue",
        "XXBTZUSD": "blue",
        "XXRPZUSD": "blue"
    };

    let algorithms = ["linear", "linear_scaled"];

    algorithms.forEach((x, i) => {
        let page = document.createElement("div");
        page.id = x;
        page.className = "page";
        page.display = "none";
        document.getElementById("content").appendChild(page);

        let opt = document.createElement("option");
        opt.value = x;
        opt.innerText = x;
        document.getElementById("select").appendChild(opt);
    });

    let parseTime = function (time) {
        let timestamp = Math.trunc(parseFloat(time));
        return new Date(timestamp)
    };

    let drawLine = function (pair, algo, props, callback) {
        props = props || {};
        props.color = props.color || "steelblue";
        d3.csv('/data.csv?pair=' + pair + '&algo=' + algo, function (d) {
            d.date = parseTime(d.timestamp);
            d.price = +d.price;
            d.predicted = +d.predicted;
            return d;
        }, function (error, data) {
            let range = slider.get()
            let min = new Date(Math.trunc(parseFloat(range[0])))
            let max = new Date(Math.trunc(parseFloat(range[1])))

            data = data.filter(function(d){
              return d.date >= min && d.date <= max
            });

            console.log(min,max,data.length)

            // Create the graph containers
            let graphContent = document.createElement("div");
            graphContent.className = "graphContent";

            let graph = document.createElement("div");
            graph.className = "graph";

            let graphLegend = document.createElement("div");
            graphLegend.className = "graphLegend";

            graphContent.appendChild(graph);
            graphContent.appendChild(graphLegend);

            document.getElementById(algo).appendChild(graphContent);

            // Create the SVG and G for each graph
            let svg = d3.select(graph).append("svg").attr("width", "960").attr("height", "500"),
                margin = {top: 20, right: 20, bottom: 30, left: 50},
                width = +svg.attr("width") - margin.left - margin.right,
                height = +svg.attr("height") - margin.top - margin.bottom,
                g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")").attr("id", "lines");

            if (error) throw error;
            // Get the actual data, price and timestamp (converted to date from above)
            let actual = data.map(function (d) {
                return {
                    price: d.price,
                    date: d.date
                }
            });

            // Get the predicted data, predicted and timestamp (converted to date from above)
            let predicted = data.map(function (d) {
                return {
                    price: d.predicted,
                    date: d.date
                }
            });


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

            // Extend the Y domain to encompass all values in actual AND predicted
            y.domain(d3.extent([].concat(data.map(function (d) {
                    return d.price;
                }), data.map(function (d) {
                    return d.predicted;
                })
            )));

            x.domain(d3.extent(data, function (d) {
                return d.date;
            }));

            g.append("g")
                .attr("transform", "translate(0," + height + ")")
                .call(d3.axisBottom(x))
                .select(".domain")
                .remove();

            g.append("g")
                .call(d3.axisLeft(y))
                .append("text")
                .attr("fill", "#000")
                .attr("transform", "rotate(-90)")
                .attr("y", 6)
                .attr("dy", "0.71em")
                .attr("text-anchor", "end")
                .text("Price ($)");

            // Create area
            let area = d3.area()
            // .interpolate("cardinal")
                .x(function (d) {
                    return x(d.date)
                })
                // .x1( function(d) { return x(predicted.date) } )
                .y0(function (d) {
                    return y(d.price)
                })
                .y1(function (d) {
                    return y(d.predicted)
                });

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
                .attr("stroke-width", 1)
                .attr("d", line);

            // Append predicted path
            g.append("path")
                .datum(predicted)
                .attr("fill", "none")
                .attr("stroke", "red")
                .attr("stroke-linejoin", "round")
                .attr("stroke-linecap", "round")
                .attr("stroke-width", 1)
                .attr("d", line);

            // Mean Error
            let ME = data.reduce((acc, d) => acc + (d.predicted - d.price), 0) / data.length;

            let MSE = data.reduce((acc, d) => acc + Math.pow(d.predicted - d.price, 2), 0) / data.length;
            // Mean Squared Error

            let p = document.createElement("p");
            p.className = pair;
            p.innerHTML = pair + " - " + algo;
            graphLegend.appendChild(p);

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
            graphLegend.appendChild(p);

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
            graphLegend.appendChild(p);

            p = document.createElement("p");
            p.innerHTML = "Mean Error: " + ME;
            graphLegend.appendChild(p);

            p = document.createElement("p");
            p.innerHTML = "Mean Squared Error: " + MSE;
            graphLegend.appendChild(p);
        });
    };

    let onPage = $("#select").val();
    $("#" + onPage).fadeIn("slow", function() {
      $("#" + onPage).empty()
      for (let key in colors) {
        drawLine(key, onPage, {color: colors[key]});
      }
    });

    $("#select").on('change', function() {
      let newPage = this.value;
      $("#" + onPage).fadeOut("slow", function() {
        $("#" + newPage).empty()
        for (let key in colors) {
            drawLine(key, newPage, {color: colors[key]});
        }
        $("#" + newPage).fadeIn("slow");
        onPage = newPage
      });
    });

});
