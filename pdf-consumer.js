const amqp = require('amqplib/callback_api');
const fs = require('fs');
const path = require('path');
const { createPDF } = require('./utils/pdf');
const { saveToDb } = require('./db-manager');

const DB_FILE_PATH = path.join(__dirname, 'db.json');
const OUT_DIR = './output';

// Đảm bảo thư mục output tồn tại
if (!fs.existsSync(OUT_DIR)) {
    fs.mkdirSync(OUT_DIR);
}

function startPdfConsumer() {
    amqp.connect('amqp://localhost', (err, connection) => {
        if (err) {
            console.error('PdfConsumer không thể kết nối tới RabbitMQ:', err);
            return setTimeout(startPdfConsumer, 5000);
        }

        connection.createChannel((err, channel) => {
            if (err) {
                console.error('PdfConsumer không thể tạo channel RabbitMQ:', err);
                return setTimeout(() => {
                    connection.close();
                    startPdfConsumer();
                }, 5000);
            }

            const pdfQueue = 'pdf_queue';
            const resultQueue = 'result_queue';

            channel.assertQueue(pdfQueue, { durable: true });
            channel.assertQueue(resultQueue, { durable: true });
            channel.prefetch(1);

            console.log('PdfConsumer đã sẵn sàng');

            channel.consume(pdfQueue, (msg) => {
                if (msg !== null) {
                    const data = JSON.parse(msg.content.toString());
                    console.log(`Đang xử lý tạo PDF cho file: ${data.originalFilePath}`);

                    data.status = 'pdf_processing';
                    saveToDb(data);

                    const baseName = path.basename(data.originalFilePath, path.extname(data.originalFilePath));
                    const pdfPath = path.join(OUT_DIR, `${baseName}_${Date.now()}.pdf`);

                    try {
                        const pdfFile = createPDF(data.translatedText);
                        const docStream = fs.createWriteStream(pdfFile);

                        // Xử lý khi docStream kết thúc
                        docStream.on('finish', () => {
                            try {
                                fs.renameSync(pdfFile, pdfPath);
                                data.pdfPath = pdfPath;
                                data.status = 'completed';
                                saveToDb(data);

                                channel.sendToQueue(resultQueue, Buffer.from(JSON.stringify({
                                    id: data.id,
                                    pdfPath: data.pdfPath,
                                    originalText: data.normalizedText || data.originalText,
                                    translatedText: data.translatedText,
                                    status: 'completed'
                                })), {
                                    persistent: true
                                });

                                // Acknowledge message
                                channel.ack(msg);
                            } catch (error) {
                                console.error(`Lỗi khi xử lý PDF: ${error}`);
                                data.status = 'pdf_failed';
                                data.error = error.message;
                                saveToDb(data);

                                // Acknowledge message dù có lỗi (có thể thay đổi logic để retry)
                                channel.ack(msg);
                            }
                        });

                        // Xử lý khi có lỗi với docStream
                        docStream.on('error', (error) => {
                            console.error(`Lỗi khi tạo PDF: ${error}`);
                            data.status = 'pdf_failed';
                            data.error = error.message;
                            saveToDb(data);

                            // Acknowledge message dù có lỗi
                            channel.ack(msg);
                        });
                        docStream.end();
                    } catch (error) {
                        console.error(`Lỗi khi khởi tạo tạo PDF: ${error}`);
                        data.status = 'pdf_failed';
                        data.error = error.message;
                        saveToDb(data);

                        // Acknowledge message dù có lỗi
                        channel.ack(msg);
                    }
                }
            }, { noAck: false });
        });
    });
}

startPdfConsumer();

module.exports = { startPdfConsumer, DB_FILE_PATH };
