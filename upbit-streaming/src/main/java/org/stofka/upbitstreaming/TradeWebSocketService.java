package org.stofka.upbitstreaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import okio.ByteString;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
public class TradeWebSocketService {

    private static final String WEBSOCKET_URL = "wss://api.upbit.com/websocket/v1";
    @Value("${app.data-file}")
    private String dataFilePath;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static List<String> markets = new ArrayList<>();

    private OkHttpClient client;
    private final KafkaSender kafkaSender;

    public TradeWebSocketService(KafkaSender kafkaSender) {
        this.kafkaSender = kafkaSender;
    }

    @PostConstruct
    public void init() {
        client = new OkHttpClient();
        Request request = new Request.Builder().url(WEBSOCKET_URL).build();
        WebSocketListener listener = new WebSocketListener() {
            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                System.out.println("✅ Trade WebSocket 연결 성공!");

                try {
                    loadMarkets();
                    sendSubscriptionMessage(webSocket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onMessage(WebSocket webSocket, ByteString bytes) {
                try {
                    String text = bytes.utf8();
                    JsonNode jsonNode = objectMapper.readTree(text);

                    String logMessage = jsonNode.toString();
                    System.out.println(logMessage);

                    kafkaSender.send("trade", logMessage);

                } catch (Exception e) {
                    System.out.println("⚠️ JSON 파싱 오류: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            @Override
            public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                System.err.println("❌ WebSocket 오류 발생: " + t.getMessage());
            }
        };

        client.newWebSocket(request, listener);
    }

    private void loadMarkets() throws IOException {
        File file = Paths.get(dataFilePath).toFile();
        JsonNode jsonNode = objectMapper.readTree(file);

        for (JsonNode node : jsonNode) {
            String market = node.get("market").asText();
            markets.add(market);
        }
        System.out.println("📌 KRW 종목 로드 완료: " + markets.size() + "개");
    }

    private static void sendSubscriptionMessage(WebSocket webSocket) throws IOException {
        List<Object> subscribeMsg = List.of(
                new SubscriptionTicket("test"),
                new SubscriptionType("trade", markets)
        );
        String jsonMessage = objectMapper.writeValueAsString(subscribeMsg);
        System.out.println("📡 전송할 구독 메시지: " + jsonMessage);

        webSocket.send(jsonMessage);
    }

    static class SubscriptionTicket {
        public String ticket;

        public SubscriptionTicket(String ticket) {
            this.ticket = ticket;
        }
    }

    static class SubscriptionType {
        public String type;
        public List<String> codes;

        public SubscriptionType(String type, List<String> codes) {
            this.type = type;
            this.codes = codes;
        }
    }
}
