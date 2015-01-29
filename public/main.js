var angularJS = require("angular"),
    angularBootstrap = require("angular-bootstrap"),
    angularRoute = require("angular-route"),
    angularSanitize = require("angular-sanitize");

// c3 = require("c3")

console.log("ctelemetry");
angular.module("ctelemetryApp", ["ngRoute", "ui.bootstrap", "ngSanitize"])
  .config(function ($routeProvider) {
    $routeProvider
      .when("/ct/main", {
        templateUrl: "views/main.html",
        controller: "MainCtrl"
      })
      .otherwise({
        redirectTo: "/ct/main"
      });
  })
  .run(function () {//(ControlClient) {
    // ControlClient.startup();
  })
  .controller("MainCtrl", function ($scope) {
    console.log("main ctrl init");
  });
