// !preview r2d3 data=list(data.frame(title = c("","","")), data.frame(title = c("","",""))),height=800
//
// r2d3: https://rstudio.github.io/r2d3
//

r2d3.svg.selectAll("svg > *").remove();
var g = r2d3.svg.selectAll()
    .data(data)
  .enter()
  .append("g")
    .attr("transform", function(d, i) {return "translate(" + 10 + "," + (i*140) + ")"});

function dx(d,i) {
   return (i * 125) + 50;
}
function dy(d,i) {
   return 90;
}
var lineFunction = d3.line()
                         .x(function(d,i) {return dx(d,i)})
                         .y(function(d,i) {return dy(d,i)})
                         .curve(d3.curveLinear);
var line = g.append("path")
                            .attr("d", function(d) {return lineFunction(d.title)})
                            .attr("stroke", "grey")
                            .attr("stroke-width", 20)
                            .attr("fill", "white");

var circles = g.selectAll().data(function (d) {
  return HTMLWidgets.dataframeToD3(d)
});
circles.enter()
    .append('circle')
    .attr("cy", function(d,i) {return dy(d,i)})
    .attr("cx", function(d,i) {return dx(d,i)})
      .attr('r',30)
      .attr('fill', 'white')
      .attr('stroke', 'grey')
      .attr('stroke-width','20px')
      ;

function wrap(text, width) {
  // https://stackoverflow.com/questions/24784302/wrapping-text-in-d3
    text.each(function () {
        var text = d3.select(this),
            words = text.text().split(/\s+/).reverse(),
            word,
            line = [],
            lineNumber = 0,
            lineHeight = 1.1, // ems
            x = text.attr("x"),
            y = text.attr("y"),
            dy = 0, //parseFloat(text.attr("dy")),
            tspan = text.text(null)
                        .append("tspan")
                        .attr("x", x)
                        .attr("y", y)
                        .attr("dy", dy + "em");
        while (word = words.pop()) {
            line.push(word);
            tspan.text(line.join(" "));
            if (tspan.node().getComputedTextLength() > width) {
                line.pop();
                tspan.text(line.join(" "));
                line = [word];
                tspan = text.append("tspan")
                            .attr("x", x)
                            .attr("y", y)
                            .attr("dy", ++lineNumber * lineHeight + dy + "em")
                            .text(word);
            }
        }
    });
}
var texts = g.selectAll().data(function(d) { return d.title}).enter().append("text")
      .text(function(d) { return d })
      .style("font-size", function(d,i) { return Math.min(20, dy(d,i)/ this.getComputedTextLength() * 20) + "px"; })
      .attr("dx", function(d,i) {return dx(d,i)-this.getComputedTextLength()/2})
      .attr("dy", "160px");

