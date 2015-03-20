var angularJS = require("angular"),
    angularBootstrap = require("angular-bootstrap"),
    angularRoute = require("angular-route"),
    angularSanitize = require("angular-sanitize");

// c3 = require("c3")

console.log("ctelemetry");
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

  .factory("ValueUpdater", function ($rootScope, getDate) {
    // TBD: instead of using valueMap, just broadcast the event
    var valueMap = {}, ws = null;

    function ensureActive() {
      if (ws && (ws.readyState == WebSocket.CONNECTING || ws.readyState == WebSocket.OPEN))
        return;
      // FIXME: use angular-websocket
      var ws = new window.WebSocket("ws://" + document.location.host + "/ws-data");
      ws.onopen = function () {
        console.log("ws: open");
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

  .controller("MainCtrl", function ($scope, $http, $routeParams, getDate, ValueUpdater) {
    var subtopic = $routeParams.rest ? "/" + $routeParams.rest : "";
    // $scope.loaded = false;
    $scope.data = [];
    $http({
      url: "/latest" + subtopic,
      method: "GET"
    }).success(function (result) {
      // $scope.loaded = true;
      var valueMap = {};
      $scope.data = result.cells.map(function (item) {
        var r = {
          topic: item[0],
          topicDisplayName: item[1],
          cell: item[2],
          cellDisplayName: item[3],
          count: item[4],
          timestamp: getDate(item[5]),
          value: item[6]
        };
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
            showDetails: function (e) {
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
  });

// TBD: display some kind of banner on the default page (or section list?)
// TBD: custom filter instead of getDate
// TBD: loading indicator, don't show 'empty' banners when the data aren't loaded yet
// TBD: CSRF (XSRF) protection
