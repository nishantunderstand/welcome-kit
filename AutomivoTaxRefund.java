package in.goindigo;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import in.goindigo.Constant.TaxRefundConstants;
import in.goindigo.model.*;
import in.goindigo.processor.RefundService;
import in.indigo.Booking.BookingComment;
import in.indigo.Booking.BookingCommit;
import in.indigo.Booking.BookingFee;
import in.indigo.Booking.BookingQueue;
import in.indigo.Processor.NavitaireSessionRequest;
import in.indigo.error.ErrorResponse;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.PredicateBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.eclipse.microprofile.config.ConfigProvider;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class AutomivoTaxRefund extends RouteBuilder {


    @Inject
    CamelContext camelContext;

    void init(@Observes StartupEvent ev) throws Exception {
        camelContext.addRoutes(new in.indigo.route.GetNavitaireToken());
    }

    @Override
    public void configure() {

        from("kafka:{{kafka.event.topic}}"
                + "?brokers={{kafka.bootstrap.servers}}"
                + "&groupId={{kafka.consumer.group-id}}"
                + "&securityProtocol=SASL_SSL"
                + "&saslMechanism=SCRAM-SHA-512"
                + "&saslJaasConfig=RAW({{kafka.sasl.jaas.config}})"
                + "&sslTruststoreLocation={{kafka.ssl.truststore.location}}"
                + "&sslTruststorePassword={{kafka.ssl.truststore.password}}")
                .routeId("TaxRefund_Kafka_Consumer")
                .filter().jsonpath(
                        "$[?(@.data.QueueCode == 'WEBTAX' || @.data.QueueCode == 'TAXAGT' || @.data.QueueCode == 'TAXCUS')]",
                        true
                )
                .log(LoggingLevel.INFO, "Message Consumed from Kafka topic with message_${body}")
                .setProperty(TaxRefundConstants.RecordLocator,jsonpath("$.data.recordLocator",String.class))
                .setProperty(TaxRefundConstants.QueueCode,jsonpath("$.data.QueueCode",String.class))
                .setProperty("apps.domain", simple("{{apps.domain}}"))
                .setProperty("apps.userId", simple("{{apps.userId}}"))
                .setProperty("apps.password", simple("{{apps.password}}"))
                .to("direct:getSessionToken").id("TaxRefund_Get_Session_Token") //
                .log(LoggingLevel.INFO, "TaxRefund Session Token Received From Navitaire Booking API  Response Code_${header.CamelHttpResponseCode}_${body}")
                .choice()
                .when(exchangeProperty("session").isNotNull())
                .to("direct:callNavitaireBookingApi").id("TaxRefund_Navitaire_Booking_API") //
                .choice()
                    .when(header(Exchange.HTTP_RESPONSE_CODE).isEqualTo(200))
                        .log(LoggingLevel.INFO, "TaxRefund Successfully Received Response From Navitaire Booking API  Response Code_${header.CamelHttpResponseCode}")
                        .to("direct:callEligibilityProcessor").id("TaxRefund_Eligibility_Processor")
                    .otherwise()
                        .unmarshal().json(JsonLibrary.Jackson, ErrorResponse.class)
                        .process(exchange -> {
                            ErrorResponse errorResponse = exchange.getIn().getBody(ErrorResponse.class);
                            exchange.setProperty("errorMessage", errorResponse.getErrors().getFirst().getMessage());
                        })
                        .log(LoggingLevel.INFO, "TaxRefund Error/Exception While Generating Navitaire Booking API Details_${exchangeProperty.errorMessage}__${header.CamelHttpResponseCode}")
                  .endChoice().end().endChoice()
                .otherwise()
                .log(LoggingLevel.INFO, "TaxRefund Error/Exception While Generating Navitaire Booking API Token_${exchangeProperty.errorMessage}__${header.CamelHttpResponseCode}")
                .end();


        from("direct:callNavitaireBookingApi").routeId("Call_Navitaire_Booking_API")
                .log(LoggingLevel.INFO, "TaxRefund Inside Navitaire ${routeId} Route")
                .removeHeaders("*")
                .setHeader("user_key", simple("{{api.userkey}}"))
                .setHeader(Exchange.CONTENT_TYPE, simple("application/json"))
                .setHeader(Exchange.HTTP_METHOD, simple("GET"))
                .setHeader("Authorization", simple("Bearer ${exchangeProperty.session}")) // Message header for Authorization
                .log(LoggingLevel.INFO, "TaxRefund Request Sent To Navitaire Booking API {{navitaire.3scale.api.host}}/api/nsk/v1/bookings/${exchangeProperty.RecordLocator}")
                .toD("{{navitaire.3scale.api.host}}/api/nsk/v1/booking/retrieve/byRecordLocator/${exchangeProperty.RecordLocator}?throwExceptionOnFailure=false")
                .id("TaxRefund_Booking_API")
                .setBody(simple("${body}"))
                .log(LoggingLevel.INFO, "TaxRefund Response Received From Navitaire ${routeId}  For ${exchangeProperty.RecordLocator} With Response  and code ${header.CamelHttpResponseCode}");

        from("direct:callEligibilityProcessor")
                .routeId("Call_Eligibility_Processor_API")
                .process("eligibilityProcessor").id("eligibilityProcessor_direct")
                .log(LoggingLevel.INFO,
                        "TaxRefund Eligibility Check Completed For Navitaire Booking API for RecordLocator=${exchangeProperty.RecordLocator}")
                .log(LoggingLevel.INFO, "RemovalQueue_Property=${exchangeProperty.removeBookingFromQueue_NoShow}")
                .log(LoggingLevel.INFO, "Eligibility_status=${exchangeProperty.isEligible}")
                .log(LoggingLevel.INFO, "isFirstFOPRestriced=${exchangeProperty.isFirstFOPRestriced}")
                //  MAIN CHOICE START
                .choice()
                    .when(exchangeProperty("removeBookingFromQueue_NoShow").isEqualTo(true))
                        .log(LoggingLevel.INFO, "In removeBookingFromQueue_NoShow == true block")
                        .to("direct:callRemoveBookingFromQueue").id("removeBookingFromQueue_NoShow")
                    .when(exchangeProperty(TaxRefundConstants.isEligible).isEqualTo(false))
                        .log(LoggingLevel.INFO, "In isEligible == false block")
                        .to("direct:callRemoveBookingFromQueue").id("removeBookingFromQueue_InEligible")
                    .otherwise()
                        .log(LoggingLevel.INFO, "TaxRefund Booking is Eligible for RecordLocator ${exchangeProperty.RecordLocator}")
                        .setBody(simple("${exchangeProperty.booking}"))
                        .setProperty("noShows", jsonpath("$.data.journeys[?(@.segments[?(@.passengerSegment[?(@.liftStatus == 3)])])]"))
                        .process("processPNR").id("processPNR_direct")
                        .log(LoggingLevel.INFO, "TaxRefund After processing PNR and calculating Refund; RecordLocator=${exchangeProperty.RecordLocator}; Stop Execution_${exchangeProperty.StopExecution_Route}")
                // NESTED CHOICE
                       .choice()
                            .when(simple("${exchangeProperty.StopExecution_Route} == false"))
                            .to("direct:callCreditShellApi").id("callCreditShellApi_direct")
                        .end().endChoice() //  closes nested choice
                .end(); // closes main choice

        from("direct:callCreditShellApi").routeId("Call_Navitaire_CREDITSHELL_API")
                .log(LoggingLevel.INFO, "Starting the route  ${routeId} and body is ${exchangeProperty.passengerTaxRefundTotal}")
                .setBody(simple("${exchangeProperty.booking}"))
                .setProperty("currencyCode", jsonpath("$.data.currencyCode", String.class))
                .split(simple("${exchangeProperty.passengerTaxRefundTotal}")).stopOnException()   // splits list into individual items
                .process(exchange -> {
                    PassengerTaxRefund passengerTaxRefund = exchange.getIn().getBody(PassengerTaxRefund.class);
                    SellFeeRequestData sellFeeRequestData = new SellFeeRequestData();
                    sellFeeRequestData.setFeeCode(passengerTaxRefund.getRefundType().toString());
                    sellFeeRequestData.setCollectedCurrencyCode(exchange.getProperty("currencyCode", String.class));
                    sellFeeRequestData.setPassengerKey(passengerTaxRefund.getPassengerNumber());
                    exchange.getIn().setBody(sellFeeRequestData);
                })
                .marshal().json(JsonLibrary.Jackson)
                .log(LoggingLevel.INFO, "SellRequestData for Record Locator ${exchangeProperty.RecordLocator} is ${body} ")
                .removeHeaders("*")
                .setHeader("user_key", simple("{{api.userkey}}"))
                .setHeader(Exchange.CONTENT_TYPE, simple("application/json"))
                .setHeader(Exchange.HTTP_METHOD, simple("POST"))
                .setHeader("Authorization", simple("Bearer ${exchangeProperty.session}"))
                .log(LoggingLevel.INFO, "request for Sell Request  for Record Locator ${exchangeProperty.RecordLocator} is ${body}")
                .toD("{{navitaire.3scale.api.host}}/api/nsk/v1/booking/fee?throwExceptionOnFailure=false")
                .id("callCreditShellApi_API")
                .log(LoggingLevel.INFO,"Response for Sell Request  for Record Locator ${exchangeProperty.RecordLocator} is ${body}")
                .choice()
                .when(header(Exchange.HTTP_RESPONSE_CODE).isNotEqualTo(201))
                .log(LoggingLevel.ERROR,
                        "Exception Sell API failed. Response Code=${header.CamelHttpResponseCode}")
                .unmarshal().json(JsonLibrary.Jackson, ErrorResponse.class)
                .process(exchange -> {
                    ErrorResponse errorResponse = exchange.getIn().getBody(ErrorResponse.class);
                    exchange.setProperty("errorMessage", errorResponse.getErrors().getFirst().getMessage());
                })
                .throwException(new RuntimeException("Sell API Failed with Error: ${exchangeProperty.errorMessage}"))
                .endChoice().end()
                .end()
                .to("direct:sellProcessing");

        from("direct:sellProcessing").routeId("sellProcessing")
                        .log(LoggingLevel.INFO, "Sell Request API Called for Record Locator ${exchangeProperty.RecordLocator}")
                        .to("direct:callBookingCommitApi").id("callBookingCommitApi_initialCommit")
                            .choice()
                                      .when(header(Exchange.HTTP_RESPONSE_CODE).in(200,201))
                                      .to("direct:callNavitaireBookingApi").id("sellProcessing_refreshBooking")
                                      .process("CreateOverrideFeeRequest").id("sellProcessing_createOverride")
                                      .to("direct:callBookingCommitApi").id("sellProcessing_finalCommit")
                                      .to("direct:processFeeRefundResultFlow").id("sellProcessing_feeRefundFlow")
                                  .otherwise()
                                    .unmarshal().json(JsonLibrary.Jackson, ErrorResponse.class)
                                    .process(exchange -> {
                                         ErrorResponse errorResponse = exchange.getIn().getBody(ErrorResponse.class);
                                          exchange.setProperty("errorMessage", errorResponse.getErrors().getFirst().getMessage());
                                    })
                                    .log(LoggingLevel.INFO, "TaxRefund Error/Exception While Generating Navitaire Booking API Details_${exchangeProperty.errorMessage}__${header.CamelHttpResponseCode}")
                        .end();

        //Booking Commit API Call
        from("direct:callBookingCommitApi").routeId("Call_Booking_Commit_API")
                .process(exchange -> {
                    BookingCommit bookingCommit =  new BookingCommit();
                    bookingCommit.setRestrictionOverride(true);
                    BookingComment bookingComment =  new BookingComment();
                    bookingComment.setType(0);
                    bookingComment.setText(exchange.getProperty("commentText",String.class));
                    bookingComment.setCreatedDate(java.time.LocalDateTime.now().toString());
                    bookingComment.setSendToBookingSource(true);
                    List<BookingComment> bookingComments = new ArrayList<>();
                    bookingComments.add(bookingComment);
                    bookingCommit.setComments(bookingComments);
                    bookingCommit.setReceivedReference("TaxRefundDroid");
                    exchange.getIn().setBody(bookingCommit);
                })
                .marshal().json(JsonLibrary.Jackson)
                .removeHeaders("*")
                .setHeader("user_key", simple("{{api.userkey}}"))
                .setHeader(Exchange.CONTENT_TYPE, simple("application/json"))
                .setHeader(Exchange.HTTP_METHOD, simple("PUT"))
                .setHeader("Authorization", simple("Bearer ${exchangeProperty.session}"))
                .log(LoggingLevel.INFO, "Booking Commit API Called for Record Locator ${exchangeProperty.RecordLocator}_${body}")
                .toD("{{navitaire.3scale.api.host}}/api/nsk/v3/booking?throwExceptionOnFailure=false")
                .id("callBookingCommitApi_API")
                .log(LoggingLevel.INFO, "Response for Booking Commit API for Record Locator ${exchangeProperty.RecordLocator} is ${body} and response code is ${header.CamelHttpResponseCode}")
                .choice()
                   // .when(PredicateBuilder.and(header(Exchange.HTTP_RESPONSE_CODE).isNotEqualTo(201), header(Exchange.HTTP_RESPONSE_CODE).isNotEqualTo(200)))
                    .when(header(Exchange.HTTP_RESPONSE_CODE).in(200,201))
                        .log(LoggingLevel.INFO, "Booking Commit API Successful for Record Locator ${exchangeProperty.RecordLocator} with Response Code=${header.CamelHttpResponseCode}")
                    .otherwise()
                        .log(LoggingLevel.ERROR,
                            "Exception Booking Commit API failed. Response Code=${header.CamelHttpResponseCode}")
                    .unmarshal().json(JsonLibrary.Jackson, ErrorResponse.class)
                    .process(exchange -> {
                        ErrorResponse errorResponse = exchange.getIn().getBody(ErrorResponse.class);
                        exchange.setProperty("errorMessage", errorResponse.getErrors().getFirst().getMessage());
                     })
                    .throwException(new RuntimeException("Booking Commit API Failed with Error: ${exchangeProperty.errorMessage}"));

        from("direct:createOverrideFeeRequest").routeId("createOverrideFeeRequest")
                        .marshal().json(JsonLibrary.Jackson)
                        .log(LoggingLevel.INFO, "createOverrideFeeRequest API Called for Record Locator_${exchangeProperty.RecordLocator}_Body_${body}_FeeKey_${exchangeProperty.feeKey}")
                        .removeHeaders("*")
                        .setHeader("user_key", simple("{{api.userkey}}"))
                        .setHeader(Exchange.CONTENT_TYPE, simple("application/json"))
                        .setHeader(Exchange.HTTP_METHOD, simple("PUT"))
                        .setHeader("Authorization", simple("Bearer ${exchangeProperty.session}"))
                        .toD("{{navitaire.3scale.api.host}}/api/nsk/v1/booking/fee/${exchangeProperty.feeKey}?throwExceptionOnFailure=false")
                        .id("createOverrideFeeRequest_API")
                        .log(LoggingLevel.INFO, "Response for createOverrideFeeRequest API for Record Locator ${exchangeProperty.RecordLocator} is ${body} and response code is ${header.CamelHttpResponseCode}")
                        .choice()
                            .when(header(Exchange.HTTP_RESPONSE_CODE).isNotEqualTo(200))
                            .log(LoggingLevel.ERROR,
                            "Exception createOverrideFeeRequest API failed. Response Code=${header.CamelHttpResponseCode}")
                            .unmarshal().json(JsonLibrary.Jackson, ErrorResponse.class)
                             .process(exchange -> {
                                 ErrorResponse errorResponse = exchange.getIn().getBody(ErrorResponse.class);
                                exchange.setProperty("errorMessage", errorResponse.getErrors().getFirst().getMessage());
                                })
                             .throwException(new RuntimeException("createOverrideFeeRequest API Failed with Error: ${exchangeProperty.errorMessage}"));;


        from("direct:callBookingComment")
                .routeId("Call_BookingComment")
                .setBody(simple("${exchangeProperty.booking}"))
                .process(exchange -> {
                    JsonNode root = exchange.getProperty("booking", JsonNode.class);
                    BookingComment bookingComment =  new BookingComment();
                    bookingComment.setType(0);
                    bookingComment.setText(exchange.getProperty("commentText",String.class));
                    bookingComment.setCreatedDate(java.time.LocalDateTime.now().toString());
                    List<BookingComment> bookingComments = new ArrayList<>();
                    bookingComments.add(bookingComment);
                    exchange.getIn().setBody(bookingComments);
                })
                .marshal().json(JsonLibrary.Jackson)
                .removeHeaders("*")
                .setHeader("user_key", simple("{{api.userkey}}"))
                .setHeader(Exchange.CONTENT_TYPE, simple("application/json"))
                .setHeader(Exchange.HTTP_METHOD, simple("POST"))
                .setHeader("Authorization", simple("Bearer ${exchangeProperty.session}"))
                .log(LoggingLevel.INFO, "Request for Booking Comment API for Record Locator ${exchangeProperty.RecordLocator} is ${body}")
                .toD("{{navitaire.3scale.api.host}}/api/nsk/v3/bookings/${exchangeProperty.RecordLocator}/comments?throwExceptionOnFailure=false")
                .id("callBookingComment_API")
                .log(LoggingLevel.INFO, "Response for Booking Comment API for Record Locator ${exchangeProperty.RecordLocator} is ${body} and response code is ${header.CamelHttpResponseCode}")
                .choice()
                    .when(header(Exchange.HTTP_RESPONSE_CODE).isNotEqualTo(201))
                    .log(LoggingLevel.ERROR,
                            "Exception Booking Comment API failed. Response Code=${header.CamelHttpResponseCode}")
                    .unmarshal().json(JsonLibrary.Jackson, ErrorResponse.class)
                    .process(exchange -> {
                        ErrorResponse errorResponse = exchange.getIn().getBody(ErrorResponse.class);
                        exchange.setProperty("errorMessage", errorResponse.getErrors().getFirst().getMessage());
                     })
                    .throwException(new RuntimeException("Booking Comment API Failed with Error: ${exchangeProperty.errorMessage}"));


// Add Payment to Booking
        from("direct:addPaymentToBooking")
                .routeId("ADD_PAYMENT_TO_BOOKING")
                .marshal().json(JsonLibrary.Jackson)
                .removeHeaders("*")
                .setHeader("user_key", simple("{{api.userkey}}"))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .setHeader("Authorization",simple("Bearer ${exchangeProperty.session}"))
                .log(LoggingLevel.INFO,"ADD_PAYMENT_TO_BOOKING_001 : Request ${body}")
                .toD("{{navitaire.3scale.api.host}}/api/nsk/v5/booking/payments?throwExceptionOnFailure=false")
                .id("addPaymentToBooking_API")
                .log(LoggingLevel.INFO,"ADD_PAYMENT_TO_BOOKING_002 : Response ${exchangeProperty.addPaymentToBookingResponse} : -> : ${body}")
                .choice()
                    .when(header(Exchange.HTTP_RESPONSE_CODE).isNotEqualTo(201))
                    .log(LoggingLevel.ERROR, "ADD_PAYMENT_TO_BOOKING_003 : Exception AddPaymentToBooking API failed. Response Code=${header.CamelHttpResponseCode}")
                    .unmarshal().json(JsonLibrary.Jackson, ErrorResponse.class)
                    .process(exchange -> {
                        ErrorResponse errorResponse = exchange.getIn().getBody(ErrorResponse.class);
                        exchange.setProperty("errorMessage", errorResponse.getErrors().getFirst().getMessage());
                    })
                    .throwException(new RuntimeException("AddPaymentToBooking API Failed with Error: ${exchangeProperty.errorMessage}"))
                ;

        // Process to Refund Fee
        from("direct:processFeeRefundResultFlow").routeId("PROCESS_FEE_REFUND_RESULT_FLOW")
                .log(LoggingLevel.INFO, "processFeeRefundResultFlow_001 : Started")
                .to("direct:callNavitaireBookingApi")
                .convertBodyTo(String.class)
                .setProperty("updatedBooking",simple("${body}"))
                .setProperty("balanceDue", jsonpath("$.data.breakdown.balanceDue"))
                .log("processFeeRefundResultFlow_003: BalanceDue : ${exchangeProperty.balanceDue}")
                .setProperty("balanceDueAfterRefund", constant(BigDecimal.ZERO))
                .choice()
                .when(simple("${exchangeProperty.balanceDue} < 0"))
                .bean(RefundService.class, "RefundToCorrespondingAccount")
                .split(simple("${exchangeProperty.payments}")).stopOnException()
                .bean(RefundService.class, "refundPayment")
                .log(LoggingLevel.INFO, "processFeeRefundResultFlow_004: AddPaymentToBooking Request Built")
                .to("direct:addPaymentToBooking")
                .log(LoggingLevel.INFO, "processFeeRefundResultFlow_005: AddPaymentToBooking API Called")
                .bean(RefundService.class, "validatePaymentResponse")
                .log(LoggingLevel.INFO, "processFeeRefundResultFlow_006: AddPaymentToBooking Success")
                .to("direct:callBookingCommitApi")
                .log(LoggingLevel.INFO, "processFeeRefundResultFlow_007: BookingCommit Called")
                .to("direct:callNavitaireBookingApi")
                .convertBodyTo(String.class)
                .setProperty("balanceDue", jsonpath("$.data.breakdown.balanceDue"))
                .log(LoggingLevel.INFO, "processFeeRefundResultFlow_008: Updated BalanceDue=${exchangeProperty.balanceDue}")
                .end()
                .choice()
                   .when(simple("${exchangeProperty.balanceDue} == 0"))
                    .to("direct:callRemoveBookingFromQueue")
                    .log("processFeeRefundResultFlow_004 : Record Locator : ${exchangeProperty.RecordLocator} : Queue Removed Successfully After Refund.")
                .otherwise()
                    .log("Record Locator : ${exchangeProperty.RecordLocator}: There is still balance due in PNR, RefundToCorrespondingAccount does not worked properly")
                .end();

        //Remove Data From Booking Queue
        from("direct:callRemoveBookingFromQueue")
                .routeId("REMOVE_BOOKING_FROM_QUEUE")
                .log(LoggingLevel.INFO, "REMOVE_BOOKING_FROM_QUEUE_001 : Started")
                .setProperty("host", simple("{{navitaire.3scale.api.host}}"))
                .setProperty("user_key",simple("{{api.userkey}}"))
                .process(exchange -> {
                    BookingQueue bookingQueue = new BookingQueue();
                    bookingQueue.setAuthorizedBy("DROID Service");
                    bookingQueue.setNotes("DROID Service");
                    bookingQueue.setQueueCode(
                            exchange.getProperty("QueueCode", String.class));
                    ObjectMapper mapper = new ObjectMapper();
                    String requestBody = mapper.writeValueAsString(bookingQueue);
                    String token = exchange.getProperty("session", String.class);
                    String host = exchange.getProperty("host", String.class);
                    String userKey = exchange.getProperty("user_key", String.class);
                    HttpClient client = HttpClient.newHttpClient();
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(
                                    host+"/api/nsk/v2/booking/queue"))
                            .header("Authorization", "Bearer " + token)
                            .header("user_key", userKey)
                            .header("Content-Type", "application/json")
                            .header("Accept", "application/json")
                            .method("DELETE", HttpRequest.BodyPublishers.ofString(requestBody))
                            .build();

                    HttpResponse<String> response = client.send(
                            request,
                            HttpResponse.BodyHandlers.ofString());

                    exchange.getMessage().setBody(response.body());
                    exchange.getMessage().setHeader(
                            Exchange.HTTP_RESPONSE_CODE,
                            response.statusCode());
                })
                .id("removeBookingFromQueue_API")
                .log(LoggingLevel.INFO,
                        "Response for Remove Booking From Queue API "
                                + "for Record Locator ${exchangeProperty.RecordLocator} "
                                + "is ${body} and response code is "
                                + "${header.CamelHttpResponseCode}")

                .choice()
                .when(header(Exchange.HTTP_RESPONSE_CODE).isNotEqualTo(200))
                .unmarshal().json(JsonLibrary.Jackson, ErrorResponse.class)
                .process(exchange -> {
                    ErrorResponse errorResponse =
                            exchange.getIn().getBody(ErrorResponse.class);

                    exchange.setProperty(
                            "errorMessage",
                            errorResponse.getErrors().getFirst().getMessage());
                })
                .throwException(new RuntimeException(
                        "Remove Booking From Queue API Failed"))
                .end();

    }
}

