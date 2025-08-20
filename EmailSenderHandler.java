package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.ses.SesClient;
import software.amazon.awssdk.services.ses.model.Body;
import software.amazon.awssdk.services.ses.model.Content;
import software.amazon.awssdk.services.ses.model.Destination;
import software.amazon.awssdk.services.ses.model.Message;
import software.amazon.awssdk.services.ses.model.SendEmailRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class EmailSenderHandler implements RequestHandler<Object, String> {

    private static final String BUCKET_NAME = "harshi-email-marketing";  // replace with your bucket name
    private static final String CONTACTS_KEY = "contacts.csv";
    private static final String TEMPLATE_KEY = "email_template.html";
    private static final String FROM_EMAIL = "krishnavamsivissavajjhala@gmail.com"; // your verified SES sender email

    private final Region region = Region.EU_NORTH_1; // Your AWS region, e.g. Europe (Stockholm)
    private final S3Client s3Client = S3Client.builder().region(region).build();
    private final SesClient sesClient = SesClient.builder().region(region).build();

    @Override
    public String handleRequest(Object event, Context context) {

        try {
            String templateHtml = readS3ObjectAsString(BUCKET_NAME, TEMPLATE_KEY);
            List<Contact> contacts = readContactsFromS3(BUCKET_NAME, CONTACTS_KEY);

            for (Contact contact : contacts) {
                String personalizedBody = templateHtml.replace("{{firstName}}", contact.getFirstName());
                sendEmail(contact.getEmail(), "Weekly Newsletter", personalizedBody);
                context.getLogger().log("Email sent to: " + contact.getEmail());
            }
            return "Successfully sent emails to " + contacts.size() + " contacts.";

        } catch (Exception e) {
            context.getLogger().log("Error: " + e.getMessage());
            return "Error sending emails: " + e.getMessage();
        }
    }

    private String readS3ObjectAsString(String bucket, String key) throws IOException {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build();
        ResponseBytes<?> objectBytes = s3Client.getObjectAsBytes(getObjectRequest);
        return new String(objectBytes.asByteArray(), StandardCharsets.UTF_8);
    }

    private List<Contact> readContactsFromS3(String bucket, String key) throws IOException {
        String csvContent = readS3ObjectAsString(bucket, key);
        BufferedReader br = new BufferedReader(new InputStreamReader(
                new java.io.ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8))));

        List<Contact> contacts = new ArrayList<>();
        br.readLine(); // skip header
        String line;
        while ((line = br.readLine()) != null) {
            String[] parts = line.split(",");
            if(parts.length >= 2) {
                contacts.add(new Contact(parts[0].trim(), parts[1].trim()));
            }
        }
        br.close();
        return contacts;
    }

    private void sendEmail(String toAddress, String subject, String htmlBody) {
        Destination destination = Destination.builder().toAddresses(toAddress).build();
        Content subjectContent = Content.builder().data(subject).build();
        Content htmlContent = Content.builder().data(htmlBody).build();
        Body body = Body.builder().html(htmlContent).build();

        Message message = Message.builder().subject(subjectContent).body(body).build();

        SendEmailRequest request = SendEmailRequest.builder()
                .destination(destination)
                .message(message)
                .source(FROM_EMAIL)
                .build();

        sesClient.sendEmail(request);
    }

    static class Contact {
        private final String firstName;
        private final String email;

        public Contact(String firstName, String email) {
            this.firstName = firstName;
            this.email = email;
        }

        public String getFirstName() { return firstName; }
        public String getEmail() { return email; }
    }
}
