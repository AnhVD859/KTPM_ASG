const amqp = require('amqplib/callback_api');
const { translate } = require('./utils/translate');
const { saveToDb } = require('./db-manager');

function normalizeText(text) {
  const lines = text.split('\n');
  let result = [];
  let currentParagraph = [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (line === '') {
      if (currentParagraph.length > 0) {
        result.push(currentParagraph.join(' '));
        currentParagraph = [];
      }
    } else {
      currentParagraph.push(line);
    }
  }

  if (currentParagraph.length > 0) {
    result.push(currentParagraph.join(' '));
  }

  return result.join('\n\n');
}

function startTranslateConsumer() {
  amqp.connect('amqp://localhost', (err, connection) => {
    if (err) {
      console.error(`TranslateConsumer không thể kết nối tới RabbitMQ:`, err);
      return setTimeout(startTranslateConsumer, 5000);
    }

    connection.createChannel((err, channel) => {
      if (err) {
        console.error(`TranslateConsumer không thể tạo channel RabbitMQ:`, err);
        return setTimeout(() => {
          connection.close();
          startTranslateConsumer();
        }, 5000);
      }

      const translationQueue = 'translation_queue';
      const pdfQueue = 'pdf_queue';
      
      channel.assertQueue(translationQueue, { durable: true });
      channel.assertQueue(pdfQueue, { durable: true });
      channel.prefetch(4);
      
      console.log(`TranslateConsumer đã sẵn sàng`);
      
      channel.consume(translationQueue, (msg) => {
        if (msg !== null) {
          const data = JSON.parse(msg.content.toString());
          console.log(`Đang xử lý dịch thuật cho file: ${data.originalFilePath}`);
          data.status = 'translation_processing';
          saveToDb(data);
          
          const normalizedText = normalizeText(data.originalText);
          
          translate(normalizedText)
            .then(translatedText => {
              data.normalizedText = normalizedText;
              data.translatedText = translatedText;
              data.status = 'translation_completed';
              saveToDb(data);
              
              channel.sendToQueue(pdfQueue, Buffer.from(JSON.stringify(data)), {
                persistent: true
              });
              
              // Acknowledge message
              channel.ack(msg);
            })
            .catch(error => {
              console.error(`Lỗi khi xử lý dịch thuật: ${error}`);
              data.status = 'translation_failed';
              data.error = error.message;
              saveToDb(data);
              
              // Acknowledge message dù có lỗi (có thể thay đổi logic để retry)
              channel.ack(msg);
            });
        }
      }, { noAck: false });
    });
  });
}

startTranslateConsumer();
