import ballerinax/mysql;
import ballerina/io;
import ballerinax/mysql.driver as _;
import ballerinax/kafka;


mysql:Client db = check new("localhost", "root", "password", "logistics_db",3306);

listener kafka:Listener Consumer_Standard = check new(kafka:DEFAULT_URL, {
    groupId: "standardDeliveryGroup",  
    topics: "standardDelivery"
});

kafka:Producer Producer_Kafka = check new(kafka:DEFAULT_URL);

type ShipmentDetails record {
    string shipmentType;
    string pickupLocation;
    string deliveryLocation;
    string preferredTimeSlot;
    string firstName;
    string lastName;
    string contactNumber;
    string trackingNumber;
};

type ShipmentConfirmation record {
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


service on Consumer_Standard{
    remote function onConsumerRecord(ShipmentDetails[] req) returns error? {
        foreach ShipmentDetails request in req {
            string estimatedDelivery = calculateDeliveryTime(request.pickupLocation, request.deliveryLocation);
            
            ShipmentConfirmation confirmation = {
                confirmationId: request.trackingNumber,
                shipmentType: request.shipmentType,
                pickupLocation: request.pickupLocation,
                deliveryLocation: request.deliveryLocation,
                estimatedDeliveryTime: estimatedDelivery,
                status: "Confirmed"
            };
            
            check Producer_Kafka->send({topic: "confirmationShipment", value: confirmation});
            io:println("shipment confirmation sent for" + request.trackingNumber);
        }
    }
    
}

function calculateDeliveryTime(string pickup_location, string delivery_location) returns string {
    int time_int = pickup_location.count() + delivery_location.count();
    time_int = time_int * 2 / 10; 
    string time = time_int.toString() + " days";
    return time;
}