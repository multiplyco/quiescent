module.exports = function (config) {
    config.set({
        browsers: ['ChromeHeadless'],
        // Pin the port: without it Karma has been observed binding 8090, where a
        // long-running local JVM listening on ::1:8090 intercepts the browser's
        // localhost connection and capture times out.
        port: 9876,
        basePath: 'target',
        files: ['browser-test.js'],
        frameworks: ['cljs-test'],
        plugins: ['karma-cljs-test', 'karma-chrome-launcher'],
        colors: true,
        logLevel: config.LOG_INFO,
        client: {
            args: ["shadow.test.karma.init"],
            singleRun: true
        }
    })
};
