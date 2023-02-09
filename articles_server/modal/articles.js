const { default: mongoose } = require('mongoose');
const Schema = mongoose.Schema;

const articles = new Schema({
  tag: String,
  content: String,
  description: String,
  link: String,
  source: String,
  title: String,
  id: String,
});
const Articles = mongoose.model('articles', articles);

module.exports = Articles;
