import puppeteer from "puppeteer-core";
import * as cheerio from "cheerio";
import { parse } from "json2csv";
import amqp from "amqplib";
import AWS from "aws-sdk";
import "dotenv/config";
import fs from "fs";

AWS.config.update({
  accessKeyId: process.env.ACCESS_KEY_ID,
  secretAccessKey: process.env.SECRET_ACCESS_KEY,
  region: process.env.REGION,
});

const rabbitmqUrl = process.env.RABBIT_MQ_URL;

const forEachSeries = async (iterable, action) => {
  try {
    for (const x of iterable) {
      await action(x);
    }
  } catch (error) {
    console.log(error);
  }
};

// async function getAdditionalInfo(itemLink) {
//   const browser = await puppeteer.launch({
//     executablePath: "/usr/bin/google-chrome",
//     headless: false,
//   });
//   const page = await browser.newPage();
//   await page.goto(itemLink);
//   await page.setViewport({ width: 1080, height: 1024 });
//   await page.waitForSelector(
//     "body > main > div.product-detail > div > div.comment-facebook"
//   );

//   const list = await page.$$("._5mdd");
//   const comments = list.map((item) => {
//     const $ = cheerio.load(item);
//     return $("span").text();
//   });
//   commentsFinal = comments.length > 0 ? comments : "";
//   return { comments: commentsFinal };
// }

const getDataFromElement = async (element) => {
  const $ = cheerio.load(element);
  // const itemLink = "https://laptop88.vn" + $(".product-img a").attr("href");
  const imagePath = $(".product-img a img").attr("src");
  const name = $(".product-info .product-title a").text();
  const oldPrice = $(
    ".product-info .product-price .price-top .old-price"
  ).text();
  const currentPrice = $(
    ".product-info .product-price .price-bottom .item-price"
  ).text();
  const groupInfo = $(".product-info .product-promotion table tbody").find(
    "tr"
  );
  const feature = {
    image: imagePath.includes("https://laptop88.vn")
      ? imagePath
      : "https://laptop88.vn" + imagePath,
    name,
    oldPrice,
    currentPrice,
  };
  for (let index = 0; index < groupInfo.length; index++) {
    const element = groupInfo[index];
    const info = $(element).text().split("\n");
    feature[info[1]] = info[2];
  }
  return feature;
};

async function sendMessage(message) {
  try {
    const connection = await amqp.connect(rabbitmqUrl);
    const channel = await connection.createChannel();
    const queueName = "logic_trigger"; // Update with your queue name

    await channel.assertQueue(queueName, { durable: false });
    channel.sendToQueue(queueName, Buffer.from(JSON.stringify(message)));

    console.log(`[x] Sent message: ${message}`);
  } catch (error) {
    console.error("Error sending message to RabbitMQ:", error);
  }
}

const crawlData = async (fileName) => {
  // Launch the browser and open a new blank page
  const browser = await puppeteer.launch({
    executablePath: "/usr/bin/google-chrome",
    headless: false,
  });
  const page = await browser.newPage();

  let finalResult = [];

  // Navigate the page to a URL
  await forEachSeries(
    Array.from({ length: 12 }, (_, i) => i + 1),
    async (pageNumber) => {
      await page.goto(`https://laptop88.vn/laptop-moi.html?page=${pageNumber}`);

      // Set screen size
      await page.setViewport({ width: 1080, height: 1024 });

      await page.waitForSelector(".product-item");

      const list = await page.$$(".product-item");

      const newResult = await Promise.all(
        list.map(async (item) => {
          const element = await item.evaluate((node) => node.innerHTML);
          return getDataFromElement(element);
        })
      );
      finalResult = [...finalResult, ...newResult];
    }
  );

  console.log(parse(finalResult));
  const finalName = fileName + ".csv";
  fs.writeFile(finalName, parse(finalResult), (err) => {});
  uploadFileToS3(parse(finalResult), finalName);
  await sendMessage({
    fileName: finalName,
  });
  await browser.close();
};

(async () => {
  try {
    const queue = "crawl_trigger";
    const connection = await amqp.connect(rabbitmqUrl);
    const channel = await connection.createChannel();

    process.once("SIGINT", async () => {
      await channel.close();
      await connection.close();
    });

    await channel.assertQueue(queue, { durable: false });
    await channel.consume(
      queue,
      (message) => {
        if (message) {
          const parsedMessage = JSON.parse(message.content.toString());
          const fileName = new Date(parsedMessage.date)
            .toLocaleString("en-GB")
            .split(",")[0]
            .replaceAll("/", "-");
          crawlData(fileName);
        }
      },
      { noAck: true }
    );

    console.log(" [*] Waiting for messages. To exit press CTRL+C");
  } catch (err) {
    console.warn(err);
  }
})();

const uploadFileToS3 = (fileContent, fileName) => {
  const s3 = new AWS.S3();

  // Specify the file path and name
  const bucketName = process.env.BUCKET_NAME;
  const key = fileName;

  // Read the file content

  // Set up the parameters for S3 upload
  const params = {
    Bucket: bucketName,
    Key: key,
    Body: fileContent,
  };

  // Upload the file to S3
  s3.upload(params, (err, data) => {
    if (err) {
      console.error("Error uploading file to S3:", err);
    } else {
      console.log("File uploaded successfully. S3 Object URL:", data.Location);
    }
  });
};
