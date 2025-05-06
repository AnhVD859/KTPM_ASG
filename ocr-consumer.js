const amqp = require('amqplib/callback_api');
const fs = require('fs');
const path = require('path');
const { image2text } = require('./utils/ocr');
const { saveToDb } = require('./db-manager');

const DB_FILE_PATH = path.join(__dirname, 'db.json');

function startOcrConsumer() {
    amqp.connect('amqp://localhost', (err, connection) => {
        if (err) {
            console.error(`OcrConsumer không thể kết nối tới RabbitMQ:`, err);
            return setTimeout(startOcrConsumer, 5000);
        }

        connection.createChannel((err, channel) => {
            if (err) {
                console.error(`OcrConsumer không thể tạo channel RabbitMQ:`, err);
                return setTimeout(() => {
                    connection.close();
                    startOcrConsumer();
                }, 5000);
            }

            const ocrQueue = 'ocr_queue';
            const translationQueue = 'translation_queue';

            channel.assertQueue(ocrQueue, { durable: true });
            channel.assertQueue(translationQueue, { durable: true });
            channel.prefetch(4);

            console.log(`OcrConsumer đã sẵn sàng`);

            channel.consume(ocrQueue, (msg) => {
                if (msg !== null) {
                    const imageData = JSON.parse(msg.content.toString());
                    console.log(`Đang xử lý OCR cho file: ${imageData.originalFilePath}`);

                    imageData.status = 'ocr_processing';
                    saveToDb(imageData);

                    image2text(imageData.originalFilePath)
                        .then(text => {
                            imageData.originalText = text;
                            imageData.status = 'ocr_completed';
                            saveToDb(imageData);

                            channel.sendToQueue(translationQueue, Buffer.from(JSON.stringify(imageData)), {
                                persistent: true
                            });

                            channel.ack(msg);
                        })
                        .catch(error => {
                            console.error(`Lỗi khi xử lý OCR: ${error}`);
                            imageData.status = 'ocr_failed';
                            imageData.error = error.message;
                            saveToDb(imageData);

                            channel.ack(msg);
                        });
                }
            }, { noAck: false });
        });
    });
}

startOcrConsumer();

module.exports = { startOcrConsumer, DB_FILE_PATH };
