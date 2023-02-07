var express = require('express');
var router = express.Router();
const Articles = require('../modal/articles');

/* GET all article. */
router.get('/', async (req, res, next) => {
    try {
      const articles = await Articles.find();
      console.log('Total send ' + articles.length)
      res.status(200).json({
        total: articles.length,
        articles,
      });
    } catch (error) {
      res.status(500).json(error);
    }
});

module.exports = router;
