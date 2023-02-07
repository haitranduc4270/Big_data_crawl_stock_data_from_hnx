var express = require('express');
var cookieParser = require('cookie-parser');
var indexRouter = require('./routes/index');

var app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
const db = require('./config/db');

app.use('/', indexRouter);


//connect database
db.connectDb();


module.exports = app;
