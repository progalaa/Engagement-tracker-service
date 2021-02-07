var AWS = require("aws-sdk");
AWS.config.update({ region: "me-south-1" });
var sqs = new AWS.SQS({ apiVersion: "2012-11-05" });
var fs = require("fs");

const variables = {
  baseQueueURL: "https://sqs.me-south-1.amazonaws.com/513721399248",
  queueName: "CrowdAnalyzer",
  enrichedQueueName: "EnrichedQueue",
};

exports.createSQSQueue = function (name) {
  var url;
  var params = { QueueName: name };

  sqs.createQueue(params, function (err, data) {
    if (err) {
      return err;
    } else {
      url = data.QueueUrl;
      console.log(url);
    }
  });

  return url;
};

exports.sendSQSMessage = function (data, queueUrl) {
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
};

exports.recieveMessagesAndEnrich = function (queueURL) {
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

      module.exports.createSQSQueue(variables.enrichedQueueName);
      module.exports.sendSQSMessage(
        newMessage,
        variables.baseQueueURL + "/" + variables.enrichedQueueName+ "\n"
      );
      var loggedMessage = new Date().toISOString().replace(/T/, ' ').replace(/\..+/, '') + ' >> ' +sum(newMessage.engagements) + '\n';
      log(loggedMessage);
    });
  });
};


function sum(obj) {
  var sum = 0;
  for (var el in obj) {
    if (obj.hasOwnProperty(el)) {
      sum += parseFloat(obj[el]);
    }
  }
  return sum;
}

function log(message) {
  fs.appendFile("log.txt", message, function (err) {
    if (err) return console.log(err);
    console.log("done");
  });
}
