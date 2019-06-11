// !preview r2d3 data=list(data.frame(title = c("DTA","About Us","Join Our Team","Recruiterbox"),href=c("https://google.com","https://google.com","https://google.com","")), data.frame(title = c("DTA","Blogs", "Help and Advice"),href=c("https://google.com","https://google.com","https://google.com")), data.frame(title = c("Domain Names","Guidelines", "Name Server Change"),href=c("https://google.com","https://google.com","https://google.com")), data.frame(title = c("Design System","Components", "Templates"),href=c("https://google.com","https://google.com","https://google.com")), data.frame(title = c("Design System","Get Started", "Download", "Community"),href=c("https://google.com","https://google.com","https://google.com", ""))),height=800,width="100%"


r2d3.svg.selectAll("svg > *").remove();
var g = r2d3.svg
  .selectAll()
  .data(data)
  .enter()
  .append("g")
  .attr("transform", function(d, i) {
    return "translate(" + 15 + "," + i * 140 + ")";
  });

function dx(d, i) {
  return i * 145 + 50;
}
function dy(d, i) {
  return 90;
}
var lineFunction = d3
  .line()
  .x(function(d, i) {
    return dx(d, i);
  })
  .y(function(d, i) {
    return dy(d, i);
  })
  .curve(d3.curveLinear);
var line = g
  .append("path")
  .attr("d", function(d) {
    return lineFunction(d.title);
  })
  .attr("stroke", "grey")
  .attr("stroke-width", 20)
  .attr("fill", "white");

var circles = g.selectAll().data(function(d) {
  return HTMLWidgets.dataframeToD3(d);
});
circles
  .enter()
  .append("circle")
  .attr("cy", function(d, i) {
    return dy(d, i);
  })
  .attr("cx", function(d, i) {
    return dx(d, i);
  })
  .attr("r", 30)
  .attr("fill", "white")
  .attr("stroke", "grey")
  .attr("stroke-width", "20px");

var texts = g
  .selectAll()
  .data(function(d) {
    return HTMLWidgets.dataframeToD3(d);
  })
  .enter()
  .append("foreignObject")
  .attr("x", function(d, i) {
    return dx(d, i)-dy(d,i)/2;
  })
  .attr("y", function(d, i) {
    return dy(d, i)+ (dy(d, i)/4)+5;
  })
  .attr("width", function(d, i) {
    return dy(d, i);
    })
  .attr("height", function(d, i) {
    return dy(d, i)/1.5;
    })
  .append("xhtml:p")
  .style("text-align","center")
  .html(function(d) {
    return d.title;
  });
