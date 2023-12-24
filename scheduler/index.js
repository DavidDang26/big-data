const schedule = require("node-schedule");
const amqp = require("amqplib");
require("dotenv").config();

const rabbitmqUrl = process.env.RABBIT_MQ_HOST; // Update with your RabbitMQ server URL

// Function to send a message to RabbitMQ
async function sendMessage(message) {
  try {
    const connection = await amqp.connect(rabbitmqUrl);
    const channel = await connection.createChannel();
    const queueName = "crawl_trigger"; // Update with your queue name

    await channel.assertQueue(queueName, { durable: false });
    channel.sendToQueue(queueName, Buffer.from(JSON.stringify(message)));

    console.log(`[x] Sent message: ${message}`);
  } catch (error) {
    console.error("Error sending message to RabbitMQ:", error);
  }
}

schedule.scheduleJob("* * * * *", async () => {
  const message = {
    date: new Date().toISOString(),
  };
  sendMessage(message);
  console.log(`Scheduled job executed at ${new Date()}`);
});
