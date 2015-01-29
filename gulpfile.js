// via: http://truongtx.me/2014/08/06/using-watchify-with-gulp-for-fast-browserify-build/
// via: http://stackoverflow.com/questions/23953779/gulp-watch-and-compile-less-files-with-import
var gulp = require("gulp");
var gulpWatch = require("gulp-watch");
var less = require("gulp-less");
var browserify = require("browserify");
var source = require("vinyl-source-stream");
var watchify = require("watchify");
var livereload = require("gulp-livereload");
var gulpif = require("gulp-if");
var prefix = require("gulp-autoprefixer");
var plumber = require("gulp-plumber");
var watch;

gulp.task("browserify-nowatch", function(){
  watch = false;
  browserifyShare();
});

gulp.task("browserify-watch", function(){
  watch = true;
  browserifyShare();
});

function browserifyShare(){
  var b = browserify({
    cache: {},
    packageCache: {},
    fullPaths: true
  });

  if(watch) {
    // if watch is enable, wrap this bundle inside watchify
    b = watchify(b);
    b.on("update", function(){
      bundleShare(b);
    });
  }

  b.add("./public/main.js");
  bundleShare(b);
}

function bundleShare(b) {
  b.bundle()
    .pipe(source("share.js"))
    .pipe(gulp.dest("./public/dist"))
    .pipe(gulpif(watch, livereload()));
}

gulp.task("less", function() {
    return gulp.src("./public/style.less")  // only compile the entry file
        .pipe(plumber())
        .pipe(less({
          paths: ["./public/less", "./node_modules/bootstrap/less"]
        }))
        .pipe(prefix("last 2 versions"), {cascade:true})
        .pipe(gulp.dest("./public/dist"))
        .pipe(livereload());
});

// define the browserify-watch as dependencies for this task
gulp.task("watch", ["less", "browserify-watch"], function(){
  // TBD: "./**/*.less"
  gulp.watch("./public/*.less",
             ["less"]);

  // Start live reload server
  livereload.listen(35729);
});

gulp.task("default", ["watch"]);
