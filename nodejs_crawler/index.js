
const axios = require('axios')
const fs = require('fs')
var parser = require('xml2json');
const cheerio = require('cheerio');
const delay = ms => new Promise((resolve, reject) => setTimeout(resolve, ms));

const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka:9092'],
})

const producer = kafka.producer()

const main = async () => {
  await producer.connect()

  while (1) {
    const result = await axios.get('https://vnexpress.net/rss/kinh-doanh.rss')
    const items = JSON.parse(parser.toJson(result.data)).rss.channel.item
    
    const articles = [];

    for (const item of items) {
      const content = await axios.get(item.link)
      const $ = cheerio.load(content.data);

      articles.push(
        {
          content: $('p').text(),
          source: 'vnexpress',
          ...item
        }
      )
      console.log('Success :' + item.link)
    }


    await producer.send({
      topic: 'article',
      messages: [{
        value: JSON.stringify(articles)
      }],
    })

    await delay(1000 * 60 * 10)
  }

  await producer.disconnect()
}

main()