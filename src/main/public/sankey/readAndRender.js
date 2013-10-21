const SVG_HEIGHT = 200;
/**
 * Created by GO22670 on 10/15/13.
 */
var margin = {top: 1, right: 1, bottom: 6, left: 1},
    width = 960 - margin.left - margin.right,
    height = SVG_HEIGHT - margin.top - margin.bottom;

var formatNumber = d3.format(",.0f"),
    format = function (d) {
        return "$"+formatNumber(d);
    },
    color = d3.scale.category20();

var svgQuery = d3.select("#queryChart").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var svgResult = d3.select("#resultChart").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var sankey1 = d3.sankey()
    .nodeWidth(15)
    .nodePadding(10)
    .size([width, height]);

var path1 = sankey1.link();

var sankey2 = d3.sankey()
    .nodeWidth(15)
    .nodePadding(10)
    .size([width, height]);

var path2 = sankey2.link();

d3.select('#nextPhase').on('click', function() {
    showNextPhase();
});

d3.select('#nextResult').on('click', function() {
    resultIndex++;
    if (resultList.length == resultIndex) resultIndex=0;
    renderCurrentResultGraph();
});

d3.select('#qbe').on('click', function() {
    var id1 = d3.select("#id1")[0][0].value;
    var id2 = d3.select("#id2")[0][0].value;
    var id3 = d3.select("#id3")[0][0].value;
    var id4 = d3.select("#id4")[0][0].value;
    var start = d3.select("#start")[0][0].value;
    var end = d3.select("#end")[0][0].value;
    console.log("value "+ id1 + " " + id2 + " " + id3 + " " + id4);

    d3.select("#resultTitle").html("Please wait...");

    d3.json("http://localhost:8085/sankey?ids="+id1+","+id2+","+id3+","+id4+"&start="+start+"&end="+end, renderQueryAndResultGraphs);
});


var currentPhase = 1;
var outerGraph, outerGraph2;
var outerLink, outerLink2;
function showNextPhase() {
    var linkHeightScalar = sankey1.getLinkHeight();

    if (currentPhase == outerGraph.links.length) currentPhase = 0;

    if (currentPhase == 0) {
        d3.select("#queryTitle").html("Overall");
    }
    else {
        var title = "Phase " + currentPhase;
        d3.select("#queryTitle").html(title);
    }
    // update link heights on query graph

    var phase = outerGraph.links[currentPhase];

    outerLink.data(phase,function (d) {
        return d.id;
    }).transition()
        .style("stroke-width", function (d) {
            return Math.max(0, linkHeightScalar * d.value);
        });
    setLinkTitles(outerLink);

    // update link heights on result graph
    var linkHeightScalar2 = sankey2.getLinkHeight();

    var phase2 = outerGraph2.links[currentPhase];

    outerLink2.data(phase2,function (d) {
        return d.id;
    }).transition()
        .style("stroke-width", function (d) {
            return Math.max(0, linkHeightScalar2 * d.value);
        });

    currentPhase++;
}

var resultList;
var resultIndex = 0;
function renderQueryAndResultGraphs(queryAndResults) {
    if (!queryAndResults || !queryAndResults.query) {
        d3.select("#resultTitle").html("No connected components for query.");
                                        return;
    }
    graph = queryAndResults.query;
    outerGraph = graph;
    var overallLinks = graph.links[0];
    sankey1
        .nodes(graph.nodes)
        .links(overallLinks)
        .layout(32);

    var link = getLinkComponent(svgQuery, overallLinks, path1);
    outerLink = link;

    addNodesAndLinks(svgQuery, graph, link, sankey1, path1);

    resultList = queryAndResults.results;
    console.log("result list " + resultList);
    renderCurrentResultGraph();
}

function renderCurrentResultGraph() {
    if (resultList) {
        console.log("result index " + resultIndex);

        d3.select("#resultTitle").html("Result #"+resultIndex);

        renderResultGraph(resultList[resultIndex]);
    }
}

function renderResultGraph(graph) {
    if  (!graph) console.log("huh? no graph for " +resultIndex );
    outerGraph2 = graph;
    var overallLinks = graph.links[0];

    sankey2 = d3.sankey()
        .nodeWidth(15)
        .nodePadding(10)
        .size([width, height]);

    path2 = sankey2.link();

    sankey2
        .nodes(graph.nodes)
        .links(overallLinks)
        .layout(32);

    svgResult.selectAll("g").remove();

    var link = getLinkComponent(svgResult, overallLinks, path2);
    outerLink2 = link;
    addNodesAndLinks(svgResult, graph, link, sankey2, path2);
}

function getLinkComponent(svg, overallLinks, path) {
  //  console.log("overallLinks " + overallLinks.length);
    return svg.append("g").selectAll(".link")
        .data(overallLinks, function (d) {
            return d.id;
        })
        .enter().append("path")
        .attr("class", "link")
        .attr("d", path)
        .style("stroke-width", function (d) {
            return Math.max(1, d.dy);
        })
        .sort(function (a, b) {
            return b.dy - a.dy;
        });
}

function setLinkTitles(link) {
    link.append("title")
        .text(function (d) {
            return d.source.name + " to " + d.target.name + " " + format(d.value);
        });
}
function addNodesAndLinks(svg, graph, link, sankey, path) {
    setLinkTitles(link);
    var node = svg.append("g").selectAll(".node")
        .data(graph.nodes)
        .enter().append("g")
        .attr("class", "node")
        .attr("transform", function (d) {
            return "translate(" + d.x + "," + d.y + ")";
        })
        .call(d3.behavior.drag()
            .origin(function (d) {
                return d;
            })
            .on("dragstart", function () {
                this.parentNode.appendChild(this);
            })
            .on("drag", dragmove));

    node.append("rect")
        .attr("height", function (d) {
            return d.dy;
        })
        .attr("width", sankey.nodeWidth())
        .style("fill", function (d) {
            return d.color = color(d.name.replace(/ .*/, ""));
        })
        .style("stroke", function (d) {
            return d3.rgb(d.color).darker(2);
        })
        .append("title")
        .text(function (d) {
            return d.name + "\n" + format(d.value);
        });

    node.append("text")
        .attr("x", -6)
        .attr("y", function (d) {
            return d.dy / 2;
        })
        .attr("dy", ".35em")
        .attr("text-anchor", "end")
        .attr("transform", null)
        .text(function (d) {
            return d.name;
        })
        .filter(function (d) {
            return d.x < width / 2;
        })
        .attr("x", 6 + sankey.nodeWidth())
        .attr("text-anchor", "start");

    // TODO show two graphs - query and result

    function dragmove(d) {
        d3.select(this).attr("transform", "translate(" + d.x + "," + (d.y = Math.max(0, Math.min(height - d.dy, d3.event.y))) + ")");
        sankey.relayout();
        link.attr("d", path);
    }
}

//d3.json("complete.json", renderQueryAndResultGraphs);
//d3.json("http://localhost:8805/sankey", renderQueryAndResultGraphs);
//d3.json("resultSmurfSequence.json", renderResultGraph);