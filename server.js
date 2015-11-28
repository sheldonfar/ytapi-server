var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var sqlite3 = require('sqlite3').verbose();

app.use(bodyParser.urlencoded({extended: true}));
app.use(function (req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});


app.post('/', function (req, res) {
    var db = new sqlite3.Database('databases/stats.db');

    //db.run("DELETE FROM vid_stats where 1=1");
    db.serialize(function () {
        var stmt = db.prepare("INSERT OR REPLACE INTO vid_stats" + "(session_id, video_id, time_from, time_to, time_total) VALUES" + "(?, ?, ?, ?, ?)");
        var stmt2 = db.prepare("INSERT OR REPLACE INTO sessions (session_id) VALUES (?)");
        req.body.intervals.forEach(function (interval) {
            stmt.run(req.body.sessionId, req.body.videoId, interval.time_from, interval.time_to, req.body.videoLength);
            stmt2.run(req.body.sessionId);
        });
        stmt.finalize();
        stmt2.finalize();

        db.each("SELECT session_id, video_id, time_from, time_to, time_total FROM vid_stats", function (err, row) {
            console.log(row.session_id + ", " + row.video_id + ", " + row.time_from + ", " + row.time_to + ", " + row.time_total);
        });
    });
    db.close();
    res.send(req.body);
});

app.get('/api', function (req, res) {
    var db = new sqlite3.Database('databases/stats.db');
    var stmt;
    if (req.query.multipleLines === 'false') {
        stmt = "SELECT vid_stats.*, group_table.time_watched, strftime('%d-%m-%Y', time_added) date_added FROM (SELECT session_id,  sum(time_to - time_from) AS time_watched FROM vid_stats GROUP BY session_id) as group_table JOIN vid_stats using (session_id) JOIN sessions ON (vid_stats.session_id = sessions.session_id)  GROUP BY sessions.session_id";
    }
    else {
        stmt = "SELECT vid_stats.*, group_table.time_watched, strftime('%d-%m-%Y', time_added) date_added FROM (SELECT session_id,  sum(time_to - time_from) AS time_watched FROM vid_stats GROUP BY session_id) as group_table JOIN vid_stats using (session_id) JOIN sessions ON (vid_stats.session_id = sessions.session_id) ";

    }
    var allRecords = function (callback) {
        db.all(stmt, function (err, all) {
            callback(err, all);
        });
    };
    allRecords(function (err, all) {
        var data = all;
        console.log(err);
        db.close();
        res.send(data);
    });
});

app.get('/api/counts', function (req, res) {
    var db = new sqlite3.Database('databases/stats.db');
    var stmt = 'SELECT count(DISTINCT session_id) as session_count, count(DISTINCT video_id) as video_count, sum(time_to - time_from) as time_watched, total_time FROM vid_stats JOIN (SELECT sum(time_total) total_time FROM (SELECT DISTINCT session_id, time_total FROM vid_stats))';

    var allRecords = function (callback) {
        db.all(stmt, function (err, all) {
            callback(err, all);
        });
    };
    allRecords(function (err, all) {
        console.log(err);
        var data = all;
        //console.log(response);
        db.close();
        res.send(data);
    });
});

app.get('/api/sessions', function (req, res) {
    var db = new sqlite3.Database('databases/stats.db');
    if (typeof req.query.sessionId !== 'undefined') {
        var stmt = "SELECT * FROM (SELECT session_id, video_id, time_total, sum(time_to - time_from) as time_watched FROM vid_stats GROUP BY session_id) JOIN sessions using (session_id) WHERE session_id = '" + req.query.sessionId + "'";
        var allRecords = function (callback) {
            db.all(stmt, function (err, all) {
                callback(err, all);
            });
        };
        allRecords(function (err, all) {
            console.log(err);
            var data = all;
            //console.log(response);
            db.close();
            res.send(data);
        });
    } else {
        var stmt = "SELECT vid_stats.session_id, video_id, GROUP_CONCAT(time_from) as all_time_from, GROUP_CONCAT(time_to) as all_time_to, sum(time_to - time_from) as time_watched, time_added FROM vid_stats JOIN sessions ON (vid_stats.session_id = sessions.session_id) GROUP BY sessions.session_id";
        var allRecords = function (callback) {
            db.all(stmt, function (err, all) {
                callback(err, all);
            });
        };
        allRecords(function (err, all) {
            console.log(err);
            var data = all;
            db.close();
            res.send(data);
        });
    }
});

app.get('/api/videos', function (req, res) {
    var db = new sqlite3.Database('databases/stats.db');
    if (typeof req.query.videoId !== 'undefined') {
        var stmt = "SELECT vid_stats.session_id, GROUP_CONCAT(time_from) as all_time_from, GROUP_CONCAT(time_to) as all_time_to FROM vid_stats WHERE video_id='" + req.query.videoId + "' GROUP BY session_id";
        var allRecords = function (callback) {
            db.all(stmt, function (err, all) {
                callback(err, all);
            });
        };
        allRecords(function (err, all) {
            console.log(err);
            var data = all;
            //console.log(response);
            db.close();
            res.send(data);
        });
    }
});

app.get('/api/videos/views', function (req, res) {
    var db = new sqlite3.Database('databases/stats.db');
        var stmt = "SELECT count(DISTINCT session_id) views_count, video_id FROM vid_stats GROUP BY video_id";
        var allRecords = function (callback) {
            db.all(stmt, function (err, all) {
                callback(err, all);
            });
        };
        allRecords(function (err, all) {
            console.log(err);
            var data = all;
            //console.log(response);
            db.close();
            res.send(data);
        });
});

app.get('/api/customquery', function (req, res) {
    var db = new sqlite3.Database('databases/stats.db');
    var stmt = req.query.customQuery;
    var allRecords = function (callback) {
        db.all(stmt, function (err, all) {
            callback(err, all);
        });
    };
    allRecords(function (err, all) {
        console.log(err);
        var data = all;
        //console.log(response);
        db.close();
        res.send(data);
    });
});

var servPort = process.env.PORT || 8888;
var server = app.listen(servPort, function () {

    var host = server.address().address;
    var port = server.address().port;

    console.log('Example app listening at http://%s:%s', host, port)

});

console.log("Server has started.");