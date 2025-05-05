const amqp = require('amqplib/callback_api');
const path = require('path');
const fs = require('fs');
const { translate } = require('./utils/translate');
const { image2text } = require('./utils/ocr');
const { createPDF } = require('./utils/pdf');
const OUT_FILE = "./output/output.pdf";

const DB_FILE_PATH = path.join(__dirname, 'db.json');

function saveToDb(data, dbFilePath) {
    const db = fs.existsSync(dbFilePath) ? JSON.parse(fs.readFileSync(dbFilePath, 'utf-8')) : [];
                    
    const index = db.findIndex(entry => entry.originalFilePath === data.originalFilePath);
    if (index !== -1) {
        db[index] = data;
    } else {
        db.push(data);
    }

    fs.writeFileSync(dbFilePath, JSON.stringify(db, null, 2));
}

function normalizeText(text) {
    // Tách text thành mảng các dòng
    const lines = text.split('\n');
    let result = [];
    let currentParagraph = [];

    for (let i = 0; i < lines.length; i++) {
        const line = lines[i].trim(); // Loại bỏ khoảng trắng thừa ở đầu và cuối

        if (line === '') {
            // Nếu gặp dòng trống (xuống dòng kép), kết thúc đoạn hiện tại
            if (currentParagraph.length > 0) {
                result.push(currentParagraph.join(' '));
                currentParagraph = [];
            }
        } else {
            // Thêm dòng vào đoạn hiện tại
            currentParagraph.push(line);
        }
    }

    // Thêm đoạn cuối nếu còn
    if (currentParagraph.length > 0) {
        result.push(currentParagraph.join(' '));
    }

    // Nối các đoạn bằng một xuống dòng
    return result.join('\n');
}

function imageConsumer() {
    amqp.connect('amqp://localhost', (error0, connection) => {
        if (error0) {
            throw error0;
        }
        connection.createChannel((error1, channel) => {
            if (error1) {
                throw error1;
            }

            const queue = 'image-processing';

            channel.assertQueue(queue, {
                durable: false
            });

            console.log(' [*] Waiting for messages in %s', queue);

            channel.consume(queue, async (msg) => {
                const { filePath } = JSON.parse(msg.content.toString());
                try {
                    // Đọc ảnh gốc
                    const imageBuffer = fs.readFileSync(filePath);
                    const text = normalizeText(await image2text(imageBuffer));
                    console.log('Extracted text:', text);
                    const viText = await translate(text);
                    console.log('VI text:', viText);
                    
                    const randomId = Math.random().toString(36).substring(2, 18);
                    const pdfFileName = `${randomId}.pdf`;
                    const pdfFilePath = path.join(__dirname, 'output', pdfFileName);

                    const pdfFile = createPDF(viText);
                    const docStream = fs.createWriteStream(pdfFile);
                    docStream.on('finish', () => {
                        try {
                            fs.renameSync(pdfFile, pdfFilePath);
                            console.log("This is PDF file: " + pdfFilePath);
                        } catch (err) {
                            console.error("Error renaming file:", err);
                        }
                    });
                    docStream.end();

                    const dbEntry = {
                        originalFilePath: filePath,
                        pdfFilePath: pdfFilePath
                    };
                    saveToDb(dbEntry, DB_FILE_PATH);
                } catch (error) {
                    console.error('Error processing message', error);
                }
            }, {
                noAck: true
            });
        });
    });
}

exports.imageConsumer = imageConsumer;
exports.DB_FILE_PATH = DB_FILE_PATH;
