const kafka = require('kafka-node');
const mqtt = require('mqtt');

// Configure MQTT and Kafka brokers
const mqttBrokerUrl = 'mqtt://api.tayrix.com:1883'; // Replace with your MQTT broker URL
const mqttUsername = 'tayrix'; // Replace with your MQTT username
const mqttPassword = 'zepto@2023'; // Replace with your MQTT password
const kafkaBrokerUrl = 'api.tayrix.com:9092'; // Replace with your Kafka broker URL
const mqttTopic = 'tayrix'; // Replace with your MQTT topic
const kafkaTopic = 'tayrix'; // Replace with your Kafka topic

// Create MQTT client
const mqttClient = mqtt.connect(mqttBrokerUrl, {
    username: mqttUsername,
    password: mqttPassword
});

mqttClient.on('connect', () => {
    console.log('Connected to MQTT broker');
    mqttClient.subscribe(mqttTopic);
});

mqttClient.on('message', (topic, message) => {
    // Forward the MQTT message to Kafka
    x = JSON.parse(message.toString())
    // console.log(x.zone.deviceid)
    // console.log(x.zone.zoneid)

    const m='{"payload":"' +btoa( message.toString()) + '","zoneid":'+ x.zone.zoneid + ', "deviceid":' +  x.zone.deviceid +'}' //converting the mqtt message to an base 64 encoding using btoa and send to the kafka topic
    console.log(m);
    kafkaProducer.send([{ topic: kafkaTopic, messages: m }], (err, data) => {
        if (err) {
            console.error('Error sending message to Kafka:', err);
        } else {
            console.log('Message sent to Kafka:', data);
        }
    });
});

// Create Kafka producer
const kafkaClient = new kafka.KafkaClient({ kafkaHost: kafkaBrokerUrl });
const kafkaProducer = new kafka.Producer(kafkaClient);

kafkaProducer.on('ready', () => {
    console.log('Connected to Kafka broker');
});

kafkaProducer.on('error', (err) => {
    console.error('Error connecting to Kafka:', err);
});
