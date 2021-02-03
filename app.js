let sqs = require("./SqsClass");
var faker = require("faker");


var quereUrl = "https://sqs.me-south-1.amazonaws.com/513721399248/CrowdAnalyzer";

/** fake data */
var fakeData = {
  id: faker.random.uuid(),
  type: "post",
  source: "facebook",
  link: "https://facebook.com/fake-post",
  username: "faker fake",
  engagements: {
    likes: faker.random.number(100),
    love: faker.random.number(100),
    haha: faker.random.number(100),
    angry: faker.random.number(100),
  },
}
/** added fake data for messages */
for (i = 0; i < 10; i++) {
  // sqs.sendMessage(fakeData, quereUrl);
}


sqs.recieveMessagesAndEnrich(quereUrl);


