window.addEventListener('load', function () {
    var debounce = function(func, wait, immediate) {
    	var timeout;
    	return function() {
    		var context = this, args = arguments;
    		var later = function() {
    			timeout = null;
    			if (!immediate) func.apply(context, args);
    		};
    		var callNow = immediate && !timeout;
    		clearTimeout(timeout);
    		timeout = setTimeout(later, wait);
    		if (callNow) func.apply(context, args);
    	};
    };

    window.addEventListener('resize', debounce(function() {
      $("#" + $("#select").val()).empty();
      for (let i in currencies) {
        drawLine(currencies[i], onPage, {color: 'blue'});
      }
    }, 750));
    let min = 1.4951196E12;
    let max = 1.5305724E12;

    $('#range-start').text(new Date(min).toLocaleString());
    $('#range-end').text(new Date(max).toLocaleString());
    var slider = noUiSlider.create(document.getElementById('slider'), {
    	start: [ min, max ],
      step: 1000 * 60 * 60,
      margin: 1000 * 60 * 60,
    	range: {
    		'min': [  min ],
    		'max': [ max ]
    	}
    });

    slider.on("slide", function() {
        let range = slider.get();
        var min = new Date(Math.trunc(parseFloat(range[0])));
        var max = new Date(Math.trunc(parseFloat(range[1])));
        $('#range-start').text(new Date(min).toLocaleString());
        $('#range-end').text(new Date(max).toLocaleString());
    });

    slider.on("set", function(){
      if(arguments[1] != 0) {
        return;
      }
      let range = slider.get();
      var min = new Date(Math.trunc(parseFloat(range[0])));
      var max = new Date(Math.trunc(parseFloat(range[1])));
      $('#range-start').text(new Date(min).toLocaleString());
      $('#range-end').text(new Date(max).toLocaleString());
      $("#" + $("#select").val()).empty();
      for (let i in currencies) {
        drawLine(currencies[i], onPage, {color: 'blue'});
      }
    });


    let currencies = ["XETHZUSD", "XLTCZUSD", "XXBTZUSD", "XXRPZUSD"];
    let algorithms = ["linear", "linear_scaled", "random_forest", "gradient_boosted_tree", "decision_tree"];

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
            let range = slider.get();
            let min = new Date(Math.trunc(parseFloat(range[0])));
            let max = new Date(Math.trunc(parseFloat(range[1])));
            data = data.filter(function(d){
              return d.date >= min && d.date <= max
            });

            // Create the graph containers

            let graphContent = document.createElement("div");
            $(graphContent).attr('data-sort', currencies.indexOf(pair));
            graphContent.className = "graphContent";

            let graph = document.createElement("div");
            graph.className = "graph";

            let graphLegend = document.createElement("div");
            graphLegend.className = "graphLegend";

            graphContent.appendChild(graph);
            graphContent.appendChild(graphLegend);

            document.getElementById(algo).appendChild(graphContent);

            let parent = $(document.getElementById(algo));
            let graphs = parent.find(".graphContent");
            graphs = graphs.sort(function(a,b){
              return +$(a).attr('data-sort') - +$(b).attr('data-sort')
            });
            $(parent).empty();
            graphs.each(function(i, graph){
              parent.append(graph)
            });

            // Create the SVG and G for each graph
            let w = $(window).width()
            let h = $(window).width() * 0.52
            if(h > $(window).height() - 175) {
              h = $(window).height() - 175;
            }
            let svg = d3.select(graph).append("svg").attr("width", w).attr("height", h),
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
            p.innerHTML = pair;
            graphLegend.appendChild(p);

            // Describe the actual price line
            p = document.createElement("p");
            p.className = pair;
            p.innerHTML = "&nbsp;&nbsp;Actual";
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
            p.innerHTML = "&nbsp;&nbsp;Predicted";
            span = document.createElement("span");
            span.style.width = "18px";
            span.style.height = "18px";
            span.style.float = "left";
            span.style.background = "red";
            p.appendChild(span);
            graphLegend.appendChild(p);

            p = document.createElement("p");
            p.innerHTML = "ME: " + ME.toFixed(6);
            graphLegend.appendChild(p);

            p = document.createElement("p");
            p.innerHTML = "MSE: " + MSE.toFixed(6);
            graphLegend.appendChild(p);


            var div = d3.select("#" + $("#select").val()).append("div").attr("class", "tooltip");
            svg.append("rect")
                .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
                .attr("class", "overlay")
                .attr("width", width)
                .attr("height", height)
                .on("mousedown", function(e) {
                  if(d3.event.button != 0) {
                    d3.event.target.style.cursor = 'pointer';
                    this.x0 = null
                    if(this.drag != null) {
                      this.drag.remove()
                    }
                    this.drag = null
                    return
                  }
                  d3.event.target.style.cursor = 'move';
                  var x0 = x.invert(d3.mouse(this)[0]);
                  x0.setMinutes(0)
                  x0.setSeconds(0)
                  this.x0 = x0;
                  div.style("display", "none");
                })
                .on("mouseup", function() {
                  if(!this.drag) {
                    return;
                  }
                  var xn = x.invert(d3.mouse(this)[0]);
                  xn.setMinutes(0)
                  xn.setSeconds(0)
                  var min = Math.min(this.x0.getTime(), xn.getTime());
                  var max = Math.max(this.x0.getTime(), xn.getTime());
                  $("#" + $("#select").val()).empty();
                  slider.set([min, max])
                  this.x0 = null;
                  this.drag = null
                }).on("mouseover", function() { div.style("display", null); })
                .on("mouseout", function() {
                  d3.event.target.style.cursor = 'pointer';
                  div.style("display", "none");
                  this.x0 = null
                  if(this.drag != null) {
                    this.drag.remove()
                  }
                  this.drag = null
                })
                .on("mousemove", function(event) {
                  if(this.x0) {
                    if(!this.drag) {
                      var drag = svg.append("rect")
                          .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
                          .attr("class", "drag")
                          .attr("width", 0)
                          .attr("height", height)
                          .style("opacity", .15)
                          .style("fill", "blue")
                      this.drag = drag
                    }
                    var xn = x.invert(d3.mouse(this)[0]);
                    xn.setMinutes(0)
                    xn.setSeconds(0)
                    var min = Math.min(x(this.x0), x(xn));
                    var max = Math.max(x(this.x0), x(xn));
                    var width = max - min;
                    this.drag.attr("width", width)
                    this.drag.attr("transform", "translate(" + (parseInt(margin.left) + min )+ "," + margin.top + ")")
                    d3.event.target.style.cursor = 'move';
                    return
                  }
                  var x0 = x.invert(d3.mouse(this)[0]);
                  x0.setMinutes(0)
                  x0.setSeconds(0)
                  var i = d3.bisector(function(d){return d.date}).left(data, x0, 1)
                  var d = data[i]
                  div.html("Time: " + d.date.toLocaleString() + "<br/>" + "Actual: " + d.price.toFixed(2) + "<br/>"  + "Predicted: " + d.predicted.toFixed(2)).style("left", (d3.event.pageX) + "px").style("top", (d3.event.pageY - 28) + "px");
                });
            });
    };

    let onPage = $("#select").val();
    $("#" + onPage).fadeIn("slow", function() {
      $("#" + onPage).empty();
      for (let i in currencies) {
        drawLine(currencies[i], onPage, {color: 'blue'});
      }
    });

    $("#select").on('change', function() {
      let newPage = this.value;
      $("#" + onPage).fadeOut("slow", function() {
        $("#" + newPage).empty();
        for (let i in currencies) {
          drawLine(currencies[i], newPage, {color: 'blue'});
        }
        $("#" + newPage).fadeIn("slow");
        onPage = newPage
      });
    });

});
