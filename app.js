const mqtt = require('mqtt');
const axios = require('axios');

let connectionJson = require('./connection_params.json');
const connectionParams = JSON.parse(JSON.stringify(connectionJson));

// API endpoint configuration
const apiUrl = connectionParams.mqtt.apiUrl; // API URL
const registerEndpoint = '/devices/gateway/register'; // API endpoint
const updateEndpoint = '/devices/gateway/traits'; // API endpoint
const apiUrl_ML = connectionParams.mqtt.apiUrl_ML; // API URL
const executePredictionEndpoint = '/execute/prediction'; // API endpoint + /gatewayName

// MQTT client configuration with username and password
const mqttOptions = {
    port: 8883,
    host: connectionParams.mqtt.server, // Update with the MQTT server hostname or IP address
    username: connectionParams.mqtt.username, // Update with the MQTT username
    password: connectionParams.mqtt.password, // Update with the MQTT password
    protocol: 'mqtts', // Use mqtts for secure MQTT communication
};
const client = mqtt.connect(mqttOptions);

// MQTT client event listeners
client.on('connect', () => {
    console.log('Connected to MQTT broker');
    client.subscribe(connectionParams.mqtt.master_control_topic, (err) => {
        console.log(`Connected to topic: ${connectionParams.mqtt.master_control_topic}`);
    });
});

client.on('message', (topic, message) => {
    console.log(`Received message from MQTT topic: ${topic}`);
    const data = message.toString(); 
    if (data) {
        try {
            const payload = data//JSON.parse(data);
            //console.log(payload);
            forwardMessageToApi(payload).catch(error => {
                console.error("Error to forwardMessageToApi:", error);
            });
        } catch (error) {
            console.error("Error to parse JSON:", error);
        }
    } else {
        console.warn("Message is empty");
    }
});

client.on('error', (error) => {
    console.error(`MQTT error: ${error}`);
});

// Function to forward message to API using Axios
const forwardMessageToApi = async (payload) => {
    let received_data = {}
    console.log('Received new data:')
    console.log(JSON.parse(payload));
    try {
        received_data = JSON.parse(payload);
    } catch (error) {
        // Handle the JSON parsing error
        console.log('Invalid Message Format Detected');
        return;
    }

    console.log("LOG ::: check data type")
    if (received_data.type == "register_settings") {
        try {
            console.log("LOG ::: register_settings")
            const response = await axios.post(`${apiUrl}${registerEndpoint}`, received_data);
            console.log('Message forwarded to API:', JSON.stringify(response.data));
        } catch (error) {
            console.error('Failed to forward message to API:', error);
        }
    }
    else if (received_data.type == "update_settings") {
        try {
            console.log("LOG ::: update_settings")
            const response = await axios.post(`${apiUrl}${updateEndpoint}`, received_data);
            console.log('Message forwarded to API:', JSON.stringify(response.data));
        } catch (error) {
            console.error('Failed to forward message to API:', error);
        }
    }
    else if (received_data.type == "predict_ml_settings") {
        console.log("LOG ::: predict_ml_settings")
        for (var device_setting of received_data.devices) {
            try {
                console.log(JSON.stringify(device_setting))
                const response = await axios.post(`${apiUrl_ML}${executePredictionEndpoint}` + "/" + received_data.id, device_setting);
                console.log('Message forwarded with Response:', JSON.stringify(response.data));
            } catch (err) {
                console.error('Failed to forward message to API:', err);
            }
        }
    } else if (received_data.type == "ml_api_command") {
        console.log("API COMMAND RECEIVED: ", JSON.stringify(received_data.data));
    }

};