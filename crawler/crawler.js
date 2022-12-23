const cheerio = require('cheerio');
const request = require('request-promise');
const fs = require('fs');
const { stringify } = require('querystring');

const options = {
  uri: 'https://s.cafef.vn/Lich-su-giao-dich-BCC-6.chn?date=01/12/2022',
  transform: function (body) {
    return cheerio.load(body);
  },
};


// const crawler = async () => {

//   const $ = await request(options);

//   console.log($.html());

// }

// crawler();


request(options).then(($) => {
  console.log($('script')[10]);
  // fs.writeFileSync('data.json', JSON.stringify("" + $('article')[0], null, 4));

}).catch(error => {
    console.log(error)
});
