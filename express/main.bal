import ballerinax/mysql;
import ballerina/io;
import ballerinax/mysql.driver as _;
import ballerinax/kafka;


mysql:Client db = check new("localhost", "root", "password", "logistics_db",3306);

listener kafka:Listener ExpressConsumer = check new(kafka:DEFAULT_URL, {
    groupId: "standardDeliveryGroup",  
    topics: "standardDelivery"
});

kafka:Producer Producer_Kafka = check new(kafka:DEFAULT_URL);

type Detial_Shipment record {
    string shipmentType;
    string pickupLocation;
    string deliveryLocation;
    string preferredTimeSlot;
    string firstName;
    string lastName;
    string contactNumber;
    string trackingNumber;
};

type Confirmation_Shipment record {
    string confirmationId;
    string shipmentType;
    string pickupLocation;
    string deliveryLocation;
    string estimatedDeliveryTime;
    string status;
};

type Customer record {
    int id;
    string firstName;
    string lastName;
    string contactNumber;
};


service on ExpressConsumer{
    remote function onConsumerRecord(Detial_Shipment[] req) returns error? {
        foreach Detial_Shipment shipment_request in req {
            string estimatedDelivery = DeliveryTime(shipment_request.pickupLocation, shipment_request.deliveryLocation);
            
            Confirmation_Shipment confirmation = {
                confirmationId: shipment_request.trackingNumber,
                shipmentType: shipment_request.shipmentType,
                pickupLocation: shipment_request.pickupLocation,
                deliveryLocation: shipment_request.deliveryLocation,
                estimatedDeliveryTime: estimatedDelivery,
                status: "Confirmed"
            };
            
            check Producer_Kafka->send({topic: "confirmationShipment", value: confirmation});
            io:println("shipment confirmation sent for" + shipment_request.trackingNumber);
        }
    }
    
}

function DeliveryTime(string pickup_location, string delivery_location) returns string {
    int time_int = pickup_location.count() + delivery_location.count();
    time_int = 3; 
    string time = time_int.toString() + " days";
    return time;
}