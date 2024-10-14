import ballerina/http;
import ballerina/io;

// Define the structure for shipment details
type ShipmentDetails record {
    string shipmentType;
    string pickupLocation;
    string deliveryLocation;
    string preferredTimeSlot;
    string firstName;
    string lastName;
    string contactNumber;
    string trackingNumber = "";  // Tracking number can be generated or left empty initially
};

// Define the structure for shipment confirmation
type ShipmentConfirmation record {
    string confirmationId;
    string shipmentType;
    string pickupLocation;
    string deliveryLocation;
    string estimatedDeliveryTime;
    string status;
};

// Define the structure for customer details
type Customer record {
    int id;
    string firstName;
    string lastName;
    string contactNumber;
};

http:Client logisticClient = check new("http://localhost:9090/logistic");

public function main() returns error? {
    
    check sendShipment(logisticClient);
}

function sendShipment(http:Client httpClient) returns error? {

    ShipmentDetails req = {
        shipmentType: "standard-delivery",
        pickupLocation: "Bangalore",
        deliveryLocation: "Mysore",
        preferredTimeSlot: "2-3 days",
        firstName: "John",
        lastName: "Doe",
        contactNumber: "1234567290"
    };

    string response = check httpClient->/sendPackage.post(req);

    
    io:println("Response: ", response);
}
