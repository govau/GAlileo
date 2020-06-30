var sigInst, canvas, $GP;
var colorBrewer2Set3 = [
  "#8dd3c7",
  "#ffffb3",
  "#bebada",
  "#fb8072",
  "#80b1d3",
  "#fdb462",
  "#b3de69",
  "#fccde5",
  "#d9d9d9",
  "#bc80bd",
  "#ccebc5",
  "#ffed6f"
]; // http://colorbrewer2.org/#type=qualitative&scheme=Set3&n=12
//Load configuration file
var config = {};

//For debug allow a config=file.json parameter to specify the config
function GetQueryStringParams(sParam, defaultVal) {
  var sPageURL = "" + window.location; //.search.substring(1);//This might be causing error in Safari?
  if (sPageURL.indexOf("?") == -1) return defaultVal;
  sPageURL = sPageURL.substr(sPageURL.indexOf("?") + 1);
  var sURLVariables = sPageURL.split("&");
  for (var i = 0; i < sURLVariables.length; i++) {
    var sParameterName = sURLVariables[i].split("=");
    if (sParameterName[0] == sParam) {
      return sParameterName[1];
    }
  }
  return defaultVal;
}

jQuery.getJSON(GetQueryStringParams("config", "config.json"), function(
  data,
  textStatus,
  jqXHR
) {
  config = data;

  if (config.type != "network") {
    //bad config
    alert("Invalid configuration settings.");
    return;
  }

  //As soon as page is ready (and data ready) set up it
  $(document).ready(setupGUI(config));
}); //End JSON Config load

// FUNCTION DECLARATIONS

Object.size = function(obj) {
  var size = 0,
    key;
  for (key in obj) {
    if (obj.hasOwnProperty(key)) size++;
  }
  return size;
};

function initSigma(config) {
  var data = config.data;

  var drawProps, graphProps, mouseProps;
  // https://github.com/jacomyal/sigma.js/wiki/Settings

  if (config.sigma && config.sigma.drawingProperties)
    drawProps = config.sigma.drawingProperties;
  else
    drawProps = {
      defaultLabelColor: "#000",
      defaultLabelSize: 10,
      defaultLabelBGColor: "#ddd",
      defaultHoverLabelBGColor: "#002147",
      defaultLabelHoverColor: "#fff",
      labelThreshold: 100,
      defaultEdgeType: "curve",
      edgeColor: "source",
      hoverFontStyle: "bold",
      fontStyle: "bold",
      activeFontStyle: "bold",
      borderSize: 2, //Something other than 0
      nodeBorderColor: "default", //exactly like this
      defaultNodeBorderColor: "#000", //Any color of your choice
      defaultBorderView: "always" //apply the default color to all nodes always (normal+hover)
    };

  if (config.sigma && config.sigma.graphProperties)
    graphProps = config.sigma.graphProperties;
  else
    graphProps = {
      minNodeSize: 1,
      maxNodeSize: 20,
      minEdgeSize: 0.2,
      maxEdgeSize: 0.5
    };

  if (config.sigma && config.sigma.mouseProperties)
    mouseProps = config.sigma.mouseProperties;
  else
    mouseProps = {
      minRatio: 0.75, // How far can we zoom out?
      maxRatio: 20 // How far can we zoom in?
    };

  var a = sigma
    .init(document.getElementById("sigma-canvas"))
    .drawingProperties(drawProps)
    .graphProperties(graphProps)
    .mouseProperties(mouseProps);
  sigInst = a;
  a.active = false;
  a.neighbors = {};
  a.detail = false;

  dataReady = function() {
    //This is called as soon as data is loaded
    a.clusters = {};
    a.iterEdges(function(b) {
      b.attr["true_color"] = b.color;
    });
    a.iterNodes(function(b) {
      //This is where we populate the array used for the group select box

      // note: index may not be consistent for all nodes. Should calculate each time.
      // note: index may not be consistent for all nodes. Should calculate each time.
      // alert(JSON.stringify(b.attr.attributes[5].val));
      // alert(b.x);
      b.attr["true_color"] = b.color;
      // if (b.id.endsWith("()")) {
      //   b.attr['drawBorder'] = true;
      // }
      a.clusters[b.attr.attributes.domain] ||
        (a.clusters[b.attr.attributes.domain] = []);
      a.clusters[b.attr.attributes.domain].push(b.id); //SAH: push id not label
    });

    // a.bind("upnodes", function(a) {
    //   nodeActive(a.content[0]);
    // });

    a.draw();
    configSigmaElements(config);
  };

  if (data.indexOf("gexf") > 0 || data.indexOf("xml") > 0)
    a.parseGexf("data/" + data, dataReady);
  else a.parseJson("data/" + data, dataReady);
  gexf = sigmaInst = null;
}

function setupGUI(config) {
  // Initialise main interface elements
  $("#agencyName").html(config.agency.name);
  // Node
  if (config.legend.nodeLabel) {
    $(".node")
      .next()
      .html(config.legend.nodeLabel);
  } else {
    //hide more information link
    $(".node").hide();
  }
  // Edge
  if (config.legend.edgeLabel) {
    $(".edge")
      .next()
      .html(config.legend.edgeLabel);
  } else {
    //hide more information link
    $(".edge").hide();
  }
  // Colours
  if (config.legend.nodeLabel) {
    $(".colours")
      .next()
      .html(config.legend.colorLabel);
  } else {
    //hide more information link
    $(".colours").hide();
  }
  $.each(config.agency.websites, function(i) {
    var li = $("<li/>")
      .css("color", colorBrewer2Set3[i])
      .addClass("ui-menu-item")
      .attr("role", "menuitem")
      .appendTo($("#websites"));

    var aaa = $("<a/>")
      .addClass("ui-all")
      .text(config.agency.websites[i])
      .attr("href", "#" + config.agency.websites[i])
      .click(function() {
        showCluster(config.agency.websites[i]);
      })
      .appendTo(li);
  });
  $GP = {
    calculating: false,
    showgroup: false
  };
  $GP.intro = $("#intro");
  $GP.minifier = $GP.intro.find("#minifier");
  $GP.mini = $("#minify");
  $GP.info = $("#attributepane");
  $GP.info_donnees = $GP.info.find(".nodeattributes");
  $GP.info_name = $GP.info.find(".name");
  $GP.info_link = $GP.info.find(".link");
  $GP.info_data = $GP.info.find(".data");
  $GP.info_close = $GP.info.find(".returntext");
  $GP.info_close2 = $GP.info.find(".close");
  $GP.info_p = $GP.info.find(".p");
  $GP.info_close.click(nodeNormal);
  $GP.info_close2.click(nodeNormal);
  $GP.form = $("#mainpanel").find("form");
  $GP.search = new Search($GP.form.find("#search"));
  if (!config.features.search.enabled == true) {
    $("#search").hide();
  }
  if (!config.features.groupSelector.enabled == true) {
    $("#attributeselect").hide();
  }
  $GP.cluster = new Cluster($GP.form.find("#attributeselect"));
  config.GP = $GP;
  initSigma(config);
}

function configSigmaElements(config) {
  $GP = config.GP;

  $GP.bg = $(sigInst._core.domElements.bg);
  $GP.bg2 = $(sigInst._core.domElements.bg2);
  var clusterList = [],
    clusterDomain;
  for (clusterDomain in sigInst.clusters) {
    clusterList.push(
      '<div style=""><a href="#' +
        clusterDomain +
        '"><div style="width:40px;height:12px;border:1px solid #fff;background:' +
        sigInst._core.graph.nodesIndex[sigInst.clusters[clusterDomain][0]]
          .color +
        ';display:inline-block"></div> ' +
        clusterDomain +
        " (" +
        sigInst.clusters[clusterDomain].length +
        " members)</a></div>"
    );
  }
  //a.sort();
  $GP.cluster.content(clusterList.join(""));

  $("#zoom")
    .find(".z")
    .each(function() {
      var a = $(this),
        b = a.attr("rel");
      a.click(function() {
        if (b == "center") {
          sigInst.position(0, 0, 1).draw();
        } else {
          var a = sigInst._core;
          sigInst.zoomTo(
            a.domElements.nodes.width / 2,
            a.domElements.nodes.height / 2,
            a.mousecaptor.ratio * ("in" == b ? 1.5 : 0.5)
          );
        }
      });
    });
  $GP.mini.click(function() {
    $GP.mini.hide();
    $GP.intro.show();
    $GP.minifier.show();
  });
  $GP.minifier.click(function() {
    $GP.intro.hide();
    $GP.minifier.hide();
    $GP.mini.show();
  });
  $GP.intro.find("#showGroups").click(function() {
    true == $GP.showgroup ? showGroups(false) : showGroups(true);
  });
  clusterList = window.location.hash.substr(1);
  if (0 < clusterList.length)
    switch (clusterList) {
      case "Groups":
        showGroups(true);
        break;
      case "information":
        $.fancybox.open($("#information"), clusterDomain);
        break;
      default:
        ($GP.search.exactMatch = true), $GP.search.search(clusterList);
        $GP.search.clean();
    }
}

function Search(a) {
  this.input = a.find("input[name=search]");
  this.state = a.find(".state");
  this.results = a.find(".results");
  this.exactMatch = false;
  this.lastSearch = "";
  this.searching = false;
  var b = this;
  this.input.focus(function() {
    var a = $(this);
    a.data("focus") || (a.data("focus", true), a.removeClass("empty"));
    b.clean();
  });
  this.input.keydown(function(a) {
    if (13 == a.which)
      return b.state.addClass("searching"), b.search(b.input.val()), false;
  });
  this.state.click(function() {
    var a = b.input.val();
    b.searching && a == b.lastSearch
      ? b.close()
      : (b.state.addClass("searching"), b.search(a));
  });
  this.dom = a;
  this.close = function() {
    this.state.removeClass("searching");
    this.results.hide();
    this.searching = false;
    this.input.val(""); //SAH -- let's erase string when we close
    nodeNormal();
  };
  this.clean = function() {
    this.results.empty().hide();
    this.state.removeClass("searching");
    this.input.val("");
  };
  this.search = function(a) {
    var b = false,
      c = [],
      b = this.exactMatch ? ("^" + a + "$").toLowerCase() : a.toLowerCase(),
      g = RegExp(b);
    this.exactMatch = false;
    this.searching = true;
    this.lastSearch = a;
    this.results.empty();
    if (2 >= a.length)
      this.results.html(
        "<i>You must search for a name with a minimum of 3 letters.</i>"
      );
    else {
      sigInst.iterNodes(function(a) {
        g.test(a.label.toLowerCase()) &&
          c.push({
            id: a.id,
            name: a.label
          });
      });
      c.length ? ((b = true), nodeActive(c[0].id)) : (b = showCluster(a));
      a = ["<b>Search Results: </b>"];
      if (1 < c.length)
        for (var d = 0, h = c.length; d < h; d++)
          a.push(
            '<a href="#' +
              c[d].name +
              '" onclick="nodeActive(\'' +
              c[d].id +
              "')\">" +
              c[d].name +
              "</a>"
          );
      0 == c.length && !b && a.push("<i>No results found.</i>");
      1 < a.length && this.results.html(a.join(""));
    }
    if (c.length != 1) this.results.show();
    if (c.length == 1) this.results.hide();
  };
}

function Cluster(a) {
  this.cluster = a;
  this.display = false;
  this.list = this.cluster.find(".list");
  this.list.empty();

  this.toggle = function() {
    this.display ? this.hide() : this.show();
  };
  this.content = function(a) {
    this.list.html(a);
    this.list.find("a").click(function() {
      var a = $(this)
        .attr("href")
        .substr(1);
      showCluster(a);
    });
  };
}
function showGroups(a) {
  a
    ? ($GP.intro.find("#showGroups").text("Hide groups"),
      $GP.bg.show(),
      $GP.bg2.hide(),
      ($GP.showgroup = true))
    : ($GP.intro.find("#showGroups").text("View Groups"),
      $GP.bg.hide(),
      $GP.bg2.show(),
      ($GP.showgroup = false));
}

function nodeNormal() {
  if (true != $GP.calculating && false != sigInst.detail) {
 sigInst.drawingProperties("edgeColor", "source");
    showGroups(false);
    $GP.calculating = true;
    sigInst.detail = true;
    $GP.info.hide();
    sigInst.iterEdges(function(a) {
      a.attr.color = false;
      a.hidden = false;
      a.color = a.attr["true_color"];
      a.attr["grey"] = false;
    });
    sigInst.iterNodes(function(a) {
      a.attr["drawBorder"] = false;
      a.hidden = false;
      a.attr.color = false;
      a.attr.lineWidth = false;
      a.attr.size = false;
      a.color = a.attr["true_color"];
      a.attr["grey"] = false;
    });
    sigInst.draw(2, 2, 2, 2);
    sigInst.neighbors = {};
    sigInst.active = false;
    $GP.calculating = false;
    window.location.hash = "";
  }
}

function nodeActive(a) {
  var groupByDirection = false;
  if (
    config.informationPanel.groupByEdgeDirection &&
    config.informationPanel.groupByEdgeDirection == true
  )
    groupByDirection = true;

  sigInst.neighbors = {};
  sigInst.detail = !0;
  var b = sigInst._core.graph.nodesIndex[a];
  showGroups(!1);
  var outgoing = {},
    incoming = {},
    mutual = {}; //SAH
  sigInst.iterEdges(function(b) {
    b.attr.lineWidth = !1;
    b.hidden = !0;

    n = {
      name: b.label,
      colour: b.color
    };

    if (a == b.source) outgoing[b.target] = n;
    //SAH
    else if (a == b.target) incoming[b.source] = n; //SAH
    if (a == b.source || a == b.target)
      sigInst.neighbors[a == b.target ? b.source : b.target] = n;
    (b.hidden = false), (b.attr.color = "rgba(0, 0, 0, 1)");
  });
  var f = [];
  sigInst.iterNodes(function(a) {
    a.hidden = true;
    a.attr.lineWidth = false;
    a.attr.color = a.color;
  });

  if (groupByDirection) {
    //SAH - Compute intersection for mutual and remove these from incoming/outgoing
    for (e in outgoing) {
      //name=outgoing[e];
      if (e in incoming) {
        mutual[e] = outgoing[e];
        delete incoming[e];
        delete outgoing[e];
      }
    }
  }

  var createList = function(c) {
    var f = [];
    var e = [],
      //c = sigInst.neighbors,
      g;
    for (g in c) {
      var d = sigInst._core.graph.nodesIndex[g];
      d.hidden = !1;
      d.attr.lineWidth = !1;
      d.attr.color = c[g].colour;
      a != g &&
        e.push({
          id: g,
          name: d.label,
          group: c[g].name ? c[g].name : "",
          colour: c[g].colour
        });
    }
    e.sort(function(a, b) {
      var c = a.group.toLowerCase(),
        d = b.group.toLowerCase(),
        e = a.name.toLowerCase(),
        f = b.name.toLowerCase();
      return c != d ? (c < d ? -1 : c > d ? 1 : 0) : e < f ? -1 : e > f ? 1 : 0;
    });
    d = "";
    for (g in e) {
      c = e[g];
      /*if (c.group != d) {
				d = c.group;
				f.push('<li class="cf" rel="' + c.color + '"><div class=""></div><div class="">' + d + "</div></li>");
			}*/
      f.push(
        '<li class="membership"><a href="#' +
          c.name +
          '" onmouseover="sigInst._core.plotter.drawHoverNode(sigInst._core.graph.nodesIndex[\'' +
          c.id +
          "'])\" onclick='return false;'" +
          // onclick=\"nodeActive('" +
          // c.id +
          // '\')"
          'onmouseout="sigInst.refresh()">' +
          c.name +
          "</a></li>"
      );
    }
    return f;
  };

  /*console.log("mutual:");
	console.log(mutual);
	console.log("incoming:");
	console.log(incoming);
	console.log("outgoing:");
	console.log(outgoing);*/

  var f = [];

  //console.log("neighbors:");
  //console.log(sigInst.neighbors);

  if (groupByDirection) {
    size = Object.size(mutual);
    f.push("<h2>Mututal (" + size + ")</h2>");
    size > 0
      ? (f = f.concat(createList(mutual)))
      : f.push("No mutual links<br>");
    size = Object.size(incoming);
    f.push("<h2>Incoming (" + size + ")</h2>");
    size > 0
      ? (f = f.concat(createList(incoming)))
      : f.push("No incoming links<br>");
    size = Object.size(outgoing);
    f.push("<h2>Outgoing (" + size + ")</h2>");
    size > 0
      ? (f = f.concat(createList(outgoing)))
      : f.push("No outgoing links<br>");
  } else {
    f = f.concat(createList(sigInst.neighbors));
  }
  //b is object of active node -- SAH
  b.hidden = false;
  b.attr.color = b.color;
  b.attr.lineWidth = 6;
  b.attr.strokeStyle = "#000000";
  sigInst.draw(2, 2, 2, 2);

  $GP.info_link.find("ul").html(f.join(""));
  $GP.info_link.find("li").each(function() {
    var a = $(this),
      b = a.attr("rel");
  });
  f = b.attr;
  if (f.attributes) {
    var image_attribute = false;
    if (config.informationPanel.imageAttribute) {
      image_attribute = config.informationPanel.imageAttribute;
    }
    e = [];
    temp_array = [];
    g = 0;
    for (var attr in f.attributes) {
      var d = f.attributes[attr],
        h = "";
      if (attr != image_attribute) {
        h = "<span><strong>" + attr + ":</strong> " + d + "</span><br/>";
      }
      //temp_array.push(f.attributes[g].attr);
      e.push(h);
    }

    if (image_attribute) {
      //image_index = jQuery.inArray(image_attribute, temp_array);
      $GP.info_name.html(
        "<div><img src=" +
          f.attributes[image_attribute] +
          ' style="vertical-align:middle" /> <span onmouseover="sigInst._core.plotter.drawHoverNode(sigInst._core.graph.nodesIndex[\'' +
          b.id +
          '\'])" onmouseout="sigInst.refresh()">' +
          b.label +
          "</span></div>"
      );
    } else {
      $GP.info_name.html(
        "<div><span onmouseover=\"sigInst._core.plotter.drawHoverNode(sigInst._core.graph.nodesIndex['" +
          b.id +
          '\'])" onmouseout="sigInst.refresh()">' +
          b.label +
          "</span></div>"
      );
    }
    // Image field for attribute pane
    $GP.info_data.html(e.join("<br/>"));
  }
  $GP.info_data.show();
  $GP.info_p.html("Connections:");
  $GP.info.show();
  $GP.info_donnees.hide();
  $GP.info_donnees.show();
  sigInst.active = a;
  window.location.hash = b.label;
}

function showCluster(a) {
  var b = sigInst.clusters[a];
  if (b && 0 < b.length) {
    //showGroups(false);
 sigInst.drawingProperties("edgeColor", "default");
    sigInst.detail = true;
    b.sort();
    sigInst.iterEdges(function(a) {
      a.hidden = false;
      a.attr.lineWidth = false;
      a.attr.color = false;
      a.color = a.attr["true_color"];
      a.attr["grey"] = 1;
    });

    sigInst.iterNodes(function(n) {
      n.attr["grey"] = 1;
      n.attr["drawBorder"] = true;
    });
    var clusterIds = [];
    var toBeMoved = [];
    for (var f = [], clusters = [], c = 0, g = b.length; c < g; c++) {
      var d = sigInst._core.graph.nodesIndex[b[c]];
      if (d.attr["grey"]) {
        clusters.push(b[c]);

        d.attr.lineWidth = true;
        f.push(
          '<li class="membership"><a href="#' +
            d.label +
            '" onmouseover="sigInst._core.plotter.drawHoverNode(sigInst._core.graph.nodesIndex[\'' +
            d.id +
            '\'])" onclick="' +
            // nodeActive('" +
            // d.id +
            // '\')
            '" onmouseout="sigInst.refresh()">' +
            d.label.replace(a, "").replace(/\/\//g, "/") +
            "</a></li>"
        );

        d.attr["drawBorder"] = false;
        if (config.agency.websites.indexOf(a)> -1) {
          d.color = colorBrewer2Set3[config.agency.websites.indexOf(a)];
        } else {
        d.color="red";

        }
        d.attr["grey"] = false;
        toBeMoved.push(
          sigInst._core.graph.nodes.findIndex(function(e) {
            return e.id == d.id;
          })
        );
        clusterIds.push(d.id);
      }
    }
    sigInst.iterEdges(function(edge) {
      if (
        clusterIds.indexOf(edge.target) >= 0 ||
        clusterIds.indexOf(edge.source) >= 0
      ) {
        if (config.agency.websites.indexOf(a)>-1) {
          d.color = colorBrewer2Set3[config.agency.websites.indexOf(a)];
        } else {
          d.color="red";

        }
        edge.attr["grey"] = false;
      }
    });
    toBeMoved.forEach(function(m) {
      moved = sigInst._core.graph.nodes.splice(m, 1);
      sigInst._core.graph.nodes.push(moved[0]);
      //sigInst._core.graph.nodesIndex[moved[0].id] = sigInst._core.graph.nodes.length;
    });

    sigInst.clusters[a] = clusters;
    sigInst.refresh();
    sigInst.draw(2, 2, 2, 2);
    $GP.info_name.html("<b>" + a + "</b>");
    $GP.info_data.hide();
    $GP.info_p.html("Pages:");
    $GP.info_link.find("ul").html(f.join(""));
    $GP.info.show();
    $GP.search.clean();
    return true;
  }
  return false;
}
