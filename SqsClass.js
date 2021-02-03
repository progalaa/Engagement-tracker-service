var AWS = require("aws-sdk");
AWS.config.update({ region: "me-south-1" });
var sqs = new AWS.SQS({ apiVersion: "2012-11-05" });

const variables = {
  baseQueueURL: "https://sqs.me-south-1.amazonaws.com/513721399248",
  queueName: "CrowdAnalyzer",
  enrichedQueueName: "EnrichedQueue",
};

function createQueue() {
  var params = { QueueName: variables.queueName };

  sqs.createQueue(params, function (err, data) {
    if (err) {
      return false;
    } else {
      console.log(data.QueueUrl);
    }
  });
}

function sendMessage(data, queueUrl) {
  var params = {
    MessageBody: JSON.stringify(data),
    QueueUrl: queueUrl,
  };
  sqs.sendMessage(params, function (err, data) {
    if (err) {
      console.log("Error", err);
    } else {
      console.log("Success", data.MessageId);
    }
  });
}

function recieveMessagesAndEnrich(queueURL) {
  var params = {
    QueueUrl: queueURL,
  };

  sqs.receiveMessage(params, function (err, data) {
    if (err) {
      console.log("Receive Error", err);
    }
    var messages = data.Messages;
    messages.forEach((message) => {
      var newMessage = JSON.parse(message.Body);
      newMessage.total_engagements = sum(newMessage.engagements);

      createQueue(variables.enrichedQueueName);
      sendMessage(
        newMessage,
        variables.baseQueueURL +'/'+variables.enrichedQueueName
      );
    });
  });
}

function sum(obj) {
  var sum = 0;
  for (var el in obj) {
    if (obj.hasOwnProperty(el)) {
      sum += parseFloat(obj[el]);
    }
  }
  return sum;
}

module.exports.createQueue = createQueue;
module.exports.sendMessage = sendMessage;
module.exports.recieveMessagesAndEnrich = recieveMessagesAndEnrich;
