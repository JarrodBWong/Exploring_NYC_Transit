// TODO: fix date math --> need to get average for the entire station
// TODO: visualization by cluster/borough (need to match each station with borough based on reverse geocoded coordinates)

var types = { "S": "Subway Stations" , "C": "CitiBike Stations"},
	w = 925,
	h = 550,
	margin = 30,
	startdate = 20141228, 
	enddate = 20150531,
	startUsage = 0,
	endUsage = 10000,
	y = d3.scaleLinear().domain([endUsage, startUsage]).range([0 + margin, h - margin]),
	x = d3.scaleLinear().domain([20141228, 20141380]).range([0 + margin -5, w]),
	days = d3.range(startdate, enddate);

var vis = d3.select("#vis")
    .append("svg:svg")  
    .attr("width", w)
    .attr("height", h)
    .append("svg:g");
			
var line = d3.line()
    .x(function(d,i) { return x(d.x); })
    .y(function(d) { return y(d.y); });

var station_types = {};
d3.text('station_types_cleaned.csv', function(text) {
    var types = d3.csvParseRows(text); 
    for (i=1; i < types.length; i++) {
        station_types[types[i][0]] = types[i][1];
    }

    var startEnd = {},
        stationCodes = {};
    d3.text('station_usage_cleaned.csv', function(text) {
        var stations = d3.csvParseRows(text);
        // console.log(station_types)
        // console.log(Object.keys(station_types))
        // console.log(station_types['1AveE15St'])
        for (i=1; i < stations.length; i++) {
            var values = stations[i].slice(2, stations[i.length-1]); // values => 154 dates
            var currData = [];

            //console.log('putting key ' + stations[i][1] + ' into stationCodes ' + ' with value ' + stations[i][0]);
            stationCodes[stations[i][1]] = stations[i][0];
            //console.log(stationCodes);
            var started = false;
            for (j=0; j < values.length; j++) {
                if (values[j] != '') {
                    currData.push({ x: days[j], y: values[j] });
                    //console.log('currData ' + currData);
                
                    if (!started) {
                        //console.log(days[j], values[j]);
                        startEnd[stations[i][1]] = { 'startdate':days[j], 'startVal':values[j] };
                        started = true;
                    } else if (j == values.length-1) {
                        startEnd[stations[i][1]]['enddate'] = days[j];
                        startEnd[stations[i][1]]['endVal'] = values[j];
                    }
                    
                }
            }
            
            // Actual line
            //console.log(stations[i][1])
            //console.log('putting ' + stations[i][1] + ' which is type ' + typeof(stations[i][1]) + ' into dictionary station_types' + ' to get value ' + station_types[stations[i][1]])
            vis.append("svg:path")
                .data([currData])
                .attr("station", stations[i][1]) // attr is stationCode
                .attr("class", station_types[stations[i][1]]) // returning undefined
                .attr("d", line)
                .on("mouseover", onmouseover)
                .on("mouseout", onmouseout);
        }
    });
    // console.log(startEnd);

    vis.append("svg:line")
        .attr("x1", x(startdate))
        .attr("y1", y(startUsage))
        .attr("x2", x(enddate))
        .attr("y2", y(startUsage))
        .attr("class", "axis")

    vis.append("svg:line")
        .attr("x1", x(startdate))
        .attr("y1", y(startUsage))
        .attr("x2", x(startdate))
        .attr("y2", y(endUsage))
        .attr("class", "axis")
                
    vis.selectAll(".xLabel")
        .data(x.ticks(5))
        .enter().append("svg:text") 
        .attr("class", "xLabel")
        .text(String)
        .attr("x", function(d) { return x(d) })
        .attr("y", h-10)
        .attr("text-anchor", "middle")

    vis.selectAll(".yLabel")
        .data(y.ticks(4))
        .enter().append("svg:text")
        .attr("class", "yLabel")
        .text(String)
        .attr("x", 0)
        .attr("y", function(d) { return y(d) })
        .attr("text-anchor", "right")
        .attr("dy", 3)
                
    vis.selectAll(".xTicks")
        .data(x.ticks(5))
        .enter().append("svg:line")
        .attr("class", "xTicks")
        .attr("x1", function(d) { return x(d); })
        .attr("y1", y(startUsage))
        .attr("x2", function(d) { return x(d); })
        .attr("y2", y(startUsage)+7) 
        
    vis.selectAll(".yTicks")
        .data(y.ticks(4))
        .enter().append("line") // ("svg:line")
        .attr("class", "yTicks")
        .attr("y1", function(d) { return y(d); })
        .attr("x1", x(startdate-0.5))
        .attr("y2", function(d) { return y(d); })
        .attr("x2", x(startdate))

    function onclick(d, i) {
        var currClass = d3.select(this).attr("class");
        if (d3.select(this).classed('selected'))
        if (d3.select(this).classed('selected')) {
             d3.select(this).attr("class", currClass.substring(0, currClass.length-9));
         } else {
             d3.select(this).classed('selected', true);
         }
    }

    function onmouseover(d, i) {
        var currClass = d3.select(this).attr("class");
        d3.select(this)
            .attr("class", currClass + " current");
        
        var stationCode = $(this).attr("station");
        var stationVals = startEnd[stationCode]; 
        // var percentChange = 100 * (stationVals['endVal'] - stationVals['startVal']) / stationVals['startVal'];

        var blurb = '<h2>' + stationCodes[stationCode] + '</h2>';
        blurb += "<p>On average, this station gets " + (Math.round(stationVals['startVal'])+Math.round(stationVals['endVal'])/2) + " entries.";
        // if (percentChange >= 0) {
        //     blurb += "an increase of " + Math.round(percentChange) + " percent."
        // } else {
        //     blurb += "a decrease of " + -1 * Math.round(percentChange) + " percent."
        // }
        // blurb += "</p>";
        
        $("#default-blurb").hide();
        $("#blurb-content").html(blurb);
    }
    function onmouseout(d, i) {
        var currClass = d3.select(this).attr("class");
        var prevClass = currClass.substring(0, currClass.length-8);
        d3.select(this)
            .attr("class", prevClass);
        // $("#blurb").text("hi again");
        $("#default-blurb").show();
        $("#blurb-content").html('');
    }

    function showType(stationType) {
        var stations = d3.selectAll("path."+stationType); // stations should be NodeList[#] ==> path.T --> 
        //console.log(stations);
        if (stations.classed('highlight')) {
            console.log('here1');
            stations.attr("class", stationType);
        } else {
            console.log('here2');
            stations.classed('highlight', true);
        }
    }

    $(document).ready(function() {
        $('#filters a').click(function() {
            var stationId = $(this).attr("id");
            //console.log('stationid is ' + stationId); // stationId => T or C
            $(this).toggleClass(stationId); 
            // we successfully get here 
            showType(stationId); // but we break here
            console.log('FINALLY MADE IT HERE'); 
        });
        
    });
});
