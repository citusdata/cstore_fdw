"use strict";

// namespace for our app
var cstore = {};

// object for building charts
cstore.chartBuilder = {};

(function(exports) {
  // defaults for now
  var highchartsDefaults = {
    chart: {
      borderWidth: 2,
      borderColor: '#666',
      type: 'column'
    },

    title: {
    },

    subtitle: {
    },

    xAxis: { },

    yAxis: {
      min: 0,
      title: { }
    }
  };

  /*
   * Extracts data and options from markup to allow most of this information to
   * reside in the markdown file containing a page's content. Expects a table
   * containing numeric data followed by a figure with a caption.
   */
  var chartOptions = function($table, $figure) {
    var categories, series, options;

    categories = $table.find('thead th').slice(1)
                       .map(function() { return $(this).text(); }).toArray();

    series = $table.find('tbody tr').map(function(idx, el) {
      var $cells = $(el).find('td');

      return {
        name: $cells.first().text(),
        data: $cells.slice(1)
                    .map(function() { return parseFloat($(this).text()); })
                    .toArray()
      };
    });

    options = $.extend(true, {}, highchartsDefaults);

    options.title.text    = $figure.attr('title');
    options.subtitle.text = $figure.find('figcaption').text();

    options.xAxis.categories = categories;
    options.yAxis.title.text = $figure.find('div').attr('title');

    options.series = series;

    return options;
  };

  /*
   * After being passed a figure preceded by a table, deduces relevant options
   * from the figure and table, removes the table, and fills the figure with a
   * fancy Highcharts chart.
   */
  exports.injectChart = function($figure) {
    var opts, $table;

    $table = $figure.prev('table');

    opts = chartOptions($table, $figure);

    $table.remove();

    $figure.highcharts(opts);
  };
}(cstore.chartBuilder));

$(function() {
  $('figure.chart').each(function() { cstore.chartBuilder.injectChart($(this)); });

  $('a[href^=#]').click(function($event) {
    var dest = $(this).attr('href');

    $event.preventDefault();

    console.log("HI");
    $('body').scrollTo(dest, 500);

    return false;
  });
});
