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
      .when("/ct/", {
        templateUrl: "views/main.html",
        controller: "MainCtrl"
      })
      .when("/ct/:rest*", {
        templateUrl: "views/main.html",
        controller: "MainCtrl"
      })
      .otherwise({
        redirectTo: "/ct/"
      });
  })
  .run(function () {//(ControlClient) {
    // ControlClient.startup();
  })
  .controller("NavBarCtrl", function ($scope, $http, $routeParams) {
    $http({
      url: "/sections",
      method: "GET"
    }).success(function (result) {
      $scope.active = function (section) {
        return section.topicPrefix == ($routeParams.rest || "/");
      };
      $scope.sections = result.sections.map(function (section) {
        return {
          topicPrefix: section[0],
          displayName: section[1]
        };
      });
      console.log("sections: %o", $scope.sections);
    });
  })
  .controller("MainCtrl", function ($scope, $http, $routeParams) {
    var subtopic = $routeParams.rest ? "/" + $routeParams.rest : "";
    function getDate (ts) {
      var d = new Date();
      d.setTime(ts * 1000);
      return d;
    }
    // $scope.loaded = false;
    $scope.data = [];
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
    console.log("main ctrl init");
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
            topic: $scope.topicMap[item[2]]
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
        $scope.$digest();
      };
      ws.onclose = function () {
        console.log("ws close");
      };
      // var ws = $websocket("/ws-data");
      // ws.onMessage(function (message) {
      //   console.log("ws: %o", message);
      // });
    });
  });

// TBD: CSRF (XSRF) protection
