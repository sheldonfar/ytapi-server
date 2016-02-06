var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var sqlite3 = require('sqlite3').verbose();
var WebSocketServer = require('ws').Server;

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
        var stmt3 = db.prepare("INSERT OR REPLACE INTO browsers (name, session_id) VALUES (?,?)");
        if (req.body.intervals) {
            req.body.intervals.forEach(function (interval) {
                stmt.run(req.body.sessionId, req.body.videoId, interval.time_from, interval.time_to, req.body.videoLength);
            });
            stmt2.run(req.body.sessionId);
            stmt3.run(req.body.browserName, req.body.sessionId);
            console.log(req.body.browserName);
            stmt.finalize();
            stmt2.finalize();
            stmt3.finalize();
        }
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

app.get('/api/browsers', function (req, res) {
    var db = new sqlite3.Database('databases/stats.db');
    var stmt = 'SELECT count(DISTINCT session_id) count, name from browsers GROUP BY name';

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

app.get('/api/videos/video', function (req, res) {
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

app.get('/api/videos/lastweek', function (req, res) {
    var db = new sqlite3.Database('databases/stats.db');
    var stmt = "SELECT count(DISTINCT sessions.session_id) views, strftime('%d-%m-%Y', time_added) date from vid_stats JOIN sessions ON (vid_stats.session_id = sessions.session_id) WHERE time_added >= date('now', '-7 days') GROUP BY date";
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

var ipaddress = process.env.OPENSHIFT_NODEJS_IP || "127.0.0.1";
var port = process.env.OPENSHIFT_NODEJS_PORT || 8888;
var server = app.listen(port, ipaddress, function () {

    var host = server.address().address;

    console.log((new Date()) + '  app listening at http://%s:%s', host, port)

});

console.log("Server has started.");

wss = new WebSocketServer({
    server: server,
    autoAcceptConnections: false
});
wss.on('connection', function (ws) {
    console.log("New connection");
    ws.on('message', function (message) {
        ws.send("Received: " + message);
    });
    ws.send('Welcome!');
});