import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class NeurIPSScraper {
    private static final String BASE_URL = "https://papers.nips.cc";
    private static final int THREAD_POOL_SIZE = 10;
    private static final String APPLICATION_NAME = "NeurIPS Paper Archiver";
    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private static final String TOKENS_DIRECTORY_PATH = "tokens";
    private static final List<String> SCOPES = Collections.singletonList(DriveScopes.DRIVE_FILE);
    private static final String CREDENTIALS_FILE_PATH = "/credentials.json";
    private static final ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static Drive driveService;

    public static void main(String[] args) {
        log("Starting NeurIPS paper archiver");
        long startTime = System.currentTimeMillis();
        try {
            initializeDriveService();
            Document mainPage = Jsoup.connect(BASE_URL).timeout(30000).get(); 
            Elements yearLinks = mainPage.select("a[href^=/paper]");
            List<String> yearUrls = yearLinks.stream()
                    .map(link -> BASE_URL + link.attr("href"))
                    .distinct()
                    .sorted(Comparator.comparingInt(NeurIPSScraper::extractYearFromUrl).reversed())
                    .collect(Collectors.toList());

            List<String> latestYearUrls = yearUrls.subList(0, Math.min(5, yearUrls.size()));
            log("Processing years: " + latestYearUrls);
            CountDownLatch latch = new CountDownLatch(latestYearUrls.size());

            latestYearUrls.forEach(url -> executor.submit(() -> {
                try {
                    processYear(url);
                } finally {
                    latch.countDown(); 
                }
            }));

            latch.await();
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES); 

            log("Total execution time: " + (System.currentTimeMillis() - startTime) + "ms");
        } catch (Exception e) {
            logError("Main process failed: " + e.getMessage());
        }
    }

    private static void processYear(String yearUrl) {
        try {
            Document yearPage = connectWithRetry(yearUrl); 
            Elements paperLinks = yearPage.select("a[href$=.html]"); 
            log("Found " + paperLinks.size() + " papers in " + yearUrl);
            CountDownLatch yearLatch = new CountDownLatch(paperLinks.size());

            paperLinks.forEach(link -> executor.submit(() -> {
                try {
                    String paperPageUrl = BASE_URL + link.attr("href");
                    processPaper(paperPageUrl); 
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); 
                } finally {
                    yearLatch.countDown(); 
                }
            }));

            yearLatch.await();
        } catch (IOException | InterruptedException e) {
            logError("Year processing failed: " + yearUrl + " - " + e.getMessage());
        }
    }

    private static void processPaper(String paperPageUrl) {
        try {
            Document abstractPage = connectWithRetry(paperPageUrl); 
            Element pdfLink = abstractPage.selectFirst("a[href$=-Paper-Conference.pdf], a[href$=-Paper.pdf]");
            
            if (pdfLink != null) {
                String pdfUrl = BASE_URL + pdfLink.attr("href");
                java.io.File downloadedFile = downloadPdf(pdfUrl);
                if (downloadedFile.exists()) {
                    uploadToGoogleDrive(downloadedFile);
                }
            } else {
                logError("PDF not found for paper page: " + paperPageUrl);
            }
        } catch (IOException e) {
            logError("Paper processing failed: " + paperPageUrl + " - " + e.getMessage());
        }
    }



    private static java.io.File downloadPdf(String pdfUrl) throws IOException {
        String fileName = pdfUrl.substring(pdfUrl.lastIndexOf('/') + 1);
        java.io.File file = new java.io.File(fileName);
        FileUtils.copyURLToFile(new URL(pdfUrl), file);  
        log("Downloaded: " + fileName);
        return file;
    }

    private static void uploadToGoogleDrive(java.io.File file) {
        try {
            com.google.api.services.drive.model.File fileMetadata = new com.google.api.services.drive.model.File();
            fileMetadata.setName(file.getName());
            FileContent mediaContent = new FileContent("application/pdf", file);
            com.google.api.services.drive.model.File uploadedFile = driveService.files()
                    .create(fileMetadata, mediaContent)
                    .setFields("id, name, webViewLink")
                    .execute();
            log("File uploaded: " + uploadedFile.getWebViewLink());

            if (file.delete()) {
                log("Deleted local file: " + file.getName());
            }
        } catch (IOException e) {
            logError("Upload failed: " + file.getName() + " - " + e.getMessage());
        }
    }

    private static void initializeDriveService() throws IOException, GeneralSecurityException {
        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        Credential credential = getCredentials(HTTP_TRANSPORT);
        driveService = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME)
                .build();
    }

    private static Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT) throws IOException {
        InputStream in = NeurIPSScraper.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
        if (in == null) {
            throw new FileNotFoundException("Credentials not found");
        }
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                .setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH)))
                .setAccessType("offline")
                .build();
        return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver.Builder().setPort(8888).build()).authorize("user");
    }

    private static Document connectWithRetry(String url) throws IOException {
        int retries = 3;
        while (retries > 0) {
            try {
                return Jsoup.connect(url).timeout(30000).get(); 
            } catch (IOException e) {
                retries--;
                if (retries == 0) throw e;  
                logError("Retrying connection to " + url);
                try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            }
        }
        throw new IOException("Failed to connect to " + url);
    }

    private static int extractYearFromUrl(String url) {
        try {
            return Integer.parseInt(url.replaceAll("\\D+", ""));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static void log(String message) {
        System.out.println("[LOG] " + dtf.format(LocalDateTime.now()) + " " + message);
    }

    private static void logError(String message) {
        System.err.println("[ERROR] " + dtf.format(LocalDateTime.now()) + " " + message);
    }
}
