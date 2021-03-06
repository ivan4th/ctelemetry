var angularJS = require("angular"),
    angularBootstrap = require("angular-bootstrap"),
    angularRoute = require("angular-route"),
    angularSanitize = require("angular-sanitize"),
    c3 = require("c3");

angular.module("ctelemetryApp", ["ngRoute", "ui.bootstrap", "ngSanitize"])
  .config(function ($locationProvider, $routeProvider) {
    $locationProvider.html5Mode(true);
    $routeProvider
      .when("/ct/main/", {
        templateUrl: "views/main.html",
        controller: "MainCtrl"
      })
      .when("/ct/main/:rest*", {
        templateUrl: "views/main.html",
        controller: "MainCtrl"
      })
      .when("/ct/log/:rest*", {
        templateUrl: "views/log.html",
        controller: "LogCtrl"
      })
      .otherwise({
        redirectTo: "/ct/main/"
      });
  })

  .run(function () {//(ControlClient) {
    // ControlClient.startup();
  })

  .factory("getDate", function () {
    return function getDate (ts) {
      var d = new Date();
      d.setTime(ts * 1000);
      return d;
    };
  })

  .filter("ctfloat", function ($filter) {
    return function (v, fractionSize) {
      return typeof v == "number" ?
        $filter("number")(v, fractionSize == null ? 4 : fractionSize).replace(",", "") : v;
    };
  })

  .directive("timeSeriesChart", function () {
    return {
      restrict: "EA",
      template: "<div></div>",
      replace: true,
      scope: {
        series: "&series"
      },
      link: function (scope, element, attrs) {
        element.addClass("time-series-chart");
        scope.$watch("series()", function (newValue) {
          if (!newValue || !newValue.length)
            return;
          newValue = newValue.concat([]).reverse();
          setTimeout(function () {
            element.html("<div></div>");
            var chart = c3.generate({
              bindto: element.find("div")[0],
              data: {
                x: "x",
                columns: [
                  ["x"].concat(newValue.map(function (item) { return item.timestamp; })),
                  ["y"].concat(newValue.map(function (item) { return item.value; }))
                ]
              },
              axis: {
                x: {
                  type: "timeseries",
                  tick: {
                    format: "%Y-%m-%d"
                  }
                }
              },
              point: {
                show: false
              },
              size: {
                width: Math.max(element[0].offsetWidth - 10 || 0, 900),
                height: 500
              }
            });
          }, 0);
        });
      }
    };
  })

  .factory("ValueUpdater", function ($rootScope, getDate) {
    // TBD: instead of using valueMap, just broadcast the event
    var valueMap = {}, ws = null;

    function ensureActive() {
      if (ws && (ws.readyState == WebSocket.CONNECTING || ws.readyState == WebSocket.OPEN))
        return;
      // FIXME: use angular-websocket
      var ws = new window.WebSocket("ws://" + document.location.host + "/ws-data");
      ws.onopen = function () {
        console.log("ws connected");
        ws.send(document.body.getAttribute("data-auth"));
      };
      ws.onmessage = function (message) {
        var d = JSON.parse(message.data);
        // console.log("ws: %o", JSON.stringify(d));
        var keyPrefix = d.topic + "|||";
        d.cells.forEach(function (cellItem) {
          var key = keyPrefix + cellItem[0],
              value = cellItem[1];
          if (valueMap.hasOwnProperty(key)) {
            // console.log("key found: " + key + "; val=" + value);
            var r = valueMap[key];
            r.count++; // FIXME: that's not quite correct either
            r.value = value;
            r.timestamp = getDate(d.ts);
          }
        });
        $rootScope.$digest();
      };
      ws.onclose = function () {
        console.log("ws close");
      };
      // var ws = $websocket("/ws-data");
      // ws.onMessage(function (message) {
      //   console.log("ws: %o", message);
      // });
    }

    return {
      ensureActive: ensureActive,
      setValueMap: function (newValueMap) {
        valueMap = newValueMap;
        ensureActive();
      }
    };
  })

  .controller("NavBarCtrl", function ($scope, $http, $routeParams, $location) {
    $scope.sections = [];
    $scope.active = function (section) {
      return section.topicPrefix == ($routeParams.rest ? "/" + $routeParams.rest : "/");
    };
    $scope.current = function () {
      for (var i = 0; i < $scope.sections.length; ++i) {
        var sec = $scope.sections[i];
        if ($scope.active(sec))
          return sec;
      }
      return null;
    };
    $scope.showingLog = function () {
      return /\/ct\/log\//.test($location.path()); // FIXME?
    };
    $http({
      url: "/sections",
      method: "GET"
    }).success(function (result) {
      $scope.sections = result.sections.map(function (section) {
        return {
          topicPrefix: section[0],
          displayName: section[1]
        };
      });
      console.log("sections: %o", $scope.sections);
    });
  })

  .controller("MainCtrl", function ($scope, $http, $routeParams, $modal, getDate, ValueUpdater) {
    var subtopic = $routeParams.rest ? "/" + $routeParams.rest : "";
    // $scope.loaded = false;
    $scope.data = [];
    $http({
      url: "/latest" + subtopic,
      method: "GET"
    }).success(function (result) {
      // $scope.loaded = true;
      var valueMap = {},
          cellMap = {};
      $scope.data = result.cells.map(function (item) {
        var cell = item[3],
            cellDisplayName = item[4],
            isMultiCell = typeof cell == "string" && /,/.test(cell),
            r = {
              topic: item[0],
              topicDisplayName: item[1],
              cellId: item[2],
              cell: cell,
              cellDisplayName: cellDisplayName,
              count: item[5],
              timestamp: getDate(item[6]),
              value: item[7],
              showDetails: function () {
                var self = this;
                console.log("cell details! " + self.cellId);
                if (isMultiCell)
                  $modal.open({
                    templateUrl: "views/multihistory.html",
                    controller: "MultiHistoryWindowCtrl",
                    size: "lg",
                    resolve: {
                      cellIds: function () {
                        return self.cellId;
                      },
                      title: function () {
                        return cellDisplayName;
                      },
                      cellDisplayNames: function () {
                        return cell.split(",").map(function (cell) {
                          return cellMap[cell] || cell;
                        });
                      }
                    }
                  });
                else
                  $modal.open({
                    templateUrl: isMultiCell ? "views/multihistory.html" : "views/history.html",
                    controller: isMultiCell ? "MultiHistoryWindowCtrl" : "HistoryWindowCtrl",
                    size: "lg",
                    resolve: {
                      cellId: function () {
                        return self.cellId;
                      },
                      cellDisplayName: function () {
                        return cellDisplayName;
                      }
                    }
                  });
              }
            };
        cellMap[cell] = cellDisplayName;
        console.log("cell %o isMultiCell %o", cell, isMultiCell);
        valueMap[r.topic + "|||" + r.cell] = r;
        return r;
      });
      ValueUpdater.setValueMap(valueMap);
    });
  })

  .controller("LogCtrl", function ($scope, $http, $routeParams, $modal, getDate) {
    var subtopic = $routeParams.rest ? "/" + $routeParams.rest : ""; // FIXME (dup)
    $scope.topicMap = {};
    $scope.topics = null;
    $scope.activeTopics = function () {
      if ($scope.topics === null)
        return null; // not loaded yet
      return $scope.topics.filter(function (t) {
        return t.enabled;
      }).map(function (t) {
        return t.id;
      }).sort().join(",");
    };
    $scope.loadLog = function () {
      var activeTopics = $scope.activeTopics();
      var filter = activeTopics === null ? "" : "?filter=" + activeTopics;
      $http({
        url: "/log" + subtopic + filter,
        method: "GET"
      }).success(function (result) {
        console.log("log: %o", result);
        var oldMap = $scope.topicMap || {};
        $scope.topicMap = {};
        $scope.topics = result.topics.map(function (item) {
          var id = item[0];
          var t = {
            id: id,
            topic: item[1],
            displayName: item[2],
            enabled: oldMap.hasOwnProperty(id) ? oldMap[id].enabled : true
          };
          $scope.topicMap[t.id] = t;
          return t;
        });

        $scope.events = result.events.map(function (item) {
          return {
            id: item[0],
            timestamp: getDate(item[1]),
            topic: $scope.topicMap[item[2]],
            showDetails: function () {
              var id = this.id;
              console.log("details! " + id);
              $modal.open({
                templateUrl: "views/event.html",
                controller: "EventWindowCtrl",
                size: "lg",
                resolve: {
                  eventId: function () {
                    return id;
                  }
                }
              });
            }
          };
        });
      });
    };
    $scope.$watch("activeTopics()", function (newValue, oldValue) {
      // avoid reloading the log after the initial load
      if (newValue === oldValue || oldValue === null)
        return;
      console.log("activeTopics=%s (was %s)", newValue, oldValue);
      $scope.loadLog();
    });
    $scope.loadLog();
  })

  .controller("EventWindowCtrl", function ($scope, $http, eventId, getDate) {
    $http({
      url: "/event/" + eventId,
      method: "GET"
    }).success(function (result) {
      console.log("event: %o", result);
      $scope.event = result;
      $scope.event.timestamp = getDate($scope.event.timestamp);
    });
  })

  .controller("HistoryWindowCtrl", function ($scope, $http, cellId, cellDisplayName, getDate) {
    $scope.mode = "chart";
    $scope.title = cellDisplayName;
    $http({
      url: "/history/" + cellId,
      method: "GET"
    }).success(function (result) {
      console.log("history: %o", result);
      $scope.history = result.history.map(function (historyItem) {
        return {
          eventId: historyItem[0],
          timestamp: getDate(historyItem[1]),
          value: historyItem[2]
        };
      });
    });
  })

  .controller("MultiHistoryWindowCtrl", function ($scope, $http, cellIds, cellDisplayNames, title, getDate) {
    $scope.title = title;
    $scope.cellDisplayNames = cellDisplayNames;
    console.log("title: %o cellDisplayNames: %o", title, cellDisplayNames);
    $http({
      url: "/history/" + cellIds,
      method: "GET"
    }).success(function (result) {
      console.log("history: %o", result);
      $scope.history = result.history.map(function (historyItem) {
        return {
          eventId: historyItem[0],
          timestamp: getDate(historyItem[1]),
          values: historyItem.slice(2)
        };
      });
    });
  });

// TBD: display some kind of banner on the default page (or section list?)
// TBD: custom filter instead of getDate
// TBD: loading indicator, don't show 'empty' banners when the data aren't loaded yet
// TBD: CSRF (XSRF) protection
