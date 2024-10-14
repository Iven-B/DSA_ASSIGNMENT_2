import ballerinax/kafka;
import ballerina/sql;
import ballerinax/mysql;
import ballerina/http;
import ballerina/uuid;
import ballerina/io;
import ballerinax/mysql.driver as _;

listener kafka:Listener logisticConsumer = check new(kafka:DEFAULT_URL, {
    groupId: "LogisticDeliveryGroup",
    topics: "confirmationShipment"
});

service /logistic on new http:Listener(9090) {
    private final mysql:Client db;
    private final kafka:Producer Producer_Kafka;

    function init() returns error? {

        self.Producer_Kafka = check new(kafka:DEFAULT_URL);
        self.db = check new("localhost", "root", "password", "logistics_db", 3306);
    }

    resource function post sendPackage(Detial_Shipment req) returns string|error? {
        // Insert new customer data (minimal error handling)
        sql:ParameterizedQuery insertCustomerQuery = `
            INSERT INTO Customers (first_name, last_name, contact_number) 
            VALUES (${req.firstName}, ${req.lastName}, ${req.contactNumber})`;
        sql:ExecutionResult _ = check self.db->execute(insertCustomerQuery);

        // Generate a tracking number for the shipment
        req.trackingNumber = uuid:createType1AsString();

        // Insert shipment details into the database
        sql:ParameterizedQuery insertShipmentQuery = `
            INSERT INTO Shipments (shipment_type, pickup_location, delivery_location, preferred_time_slot, tracking_number) 
            VALUES (${req.shipmentType}, ${req.pickupLocation}, ${req.deliveryLocation}, ${req.preferredTimeSlot}, ${req.trackingNumber})`;
        sql:ExecutionResult _ = check self.db->execute(insertShipmentQuery);

        // Produce shipment details to Kafka
        check self.Producer_Kafka->send({topic: req.shipmentType, value: req});
        return string `Shipment Details: Tracking Number${req.trackingNumber}\n Shipment Type ${req.shipmentType} \n Customer Name ${req.firstName} ${req.lastName} \n Contact Number ${req.contactNumber}`;
    }
}

service on logisticConsumer {
    private final mysql:Client db;

    function init() returns error? {
        // Initialize MySQL client
        self.db = check new("localhost", "root", "password", "logistics_db", 3306);
    }

    remote function onConsumerRecord(Confirmation_Shipment[] confirmations) returns error? {
        // Process each shipment confirmation
        foreach Confirmation_Shipment confirmation in confirmations {
            sql:ParameterizedQuery updateQuery = `
                UPDATE Shipments SET status = "confirmed", estimated_delivery_time = ${confirmation.estimatedDeliveryTime} 
                WHERE tracking_number = ${confirmation.confirmationId}`;
            sql:ExecutionResult _ = check self.db->execute(updateQuery);
        }
        io:println(confirmations);
    }
}

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


