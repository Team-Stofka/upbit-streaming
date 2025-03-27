package org.example.upbitstreaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import okio.ByteString;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Service
public class WebSocketService {

    private static final String WEBSOCKET_URL = "wss://api.upbit.com/websocket/v1";
    private static final String DATA_FILE = "C:\\Users\\2-32\\IdeaProjects\\upbit-streaming\\upbit-streaming\\src\\main\\resources\\data.json"; // JSON 파일 위치
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static List<String> markets = new ArrayList<>();

    private OkHttpClient client;

    @PostConstruct
    public void init() {
        client = new OkHttpClient();
        Request request = new Request.Builder().url(WEBSOCKET_URL).build();
        WebSocketListener listener = new WebSocketListener() {
            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                System.out.println("✅ WebSocket 연결 성공!");

                try {
                    // 시장 정보 로드
                    loadMarkets();

                    // 구독 메시지 전송
                    sendSubscriptionMessage(webSocket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onMessage(WebSocket webSocket, ByteString bytes) {
                try {
                    // 바이너리 데이터를 UTF-8로 변환
                    String text = bytes.utf8();
                    System.out.println("📩 받은 데이터: " + text);  // 받은 데이터 출력

                    // JSON 파싱
                    JsonNode jsonNode = objectMapper.readTree(text);

                    // 코드와 거래 가격 추출
                    String market = jsonNode.get("code").asText();
                    double tradePrice = 1;  // 기본 값 설정

                    // tradePrice가 JSON에서 제공되면 값을 업데이트
                    JsonNode priceNode = jsonNode.get("trade_price");
                    if (priceNode != null && !priceNode.isNull()) {
                        tradePrice = priceNode.asDouble();
                    }

                    // 현재 시간 가져오기 (UTC 기준)
                    String timestamp = Instant.now().atOffset(ZoneOffset.UTC)
                            .format(DateTimeFormatter.ISO_INSTANT);

                    // 메시지 형식 지정
                    String logMessage = market + " : " + tradePrice + " (" + timestamp + ")";
                    System.out.println(logMessage);

                    // 추가적으로 Kafka로 메시지 전송 로직을 구현할 수 있음
                    // sendToKafka(logMessage);

                } catch (Exception e) {
                    // 예외 발생 시 오류 메시지 출력
                    System.out.println("⚠️ JSON 파싱 오류: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            @Override
            public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                System.err.println("❌ WebSocket 오류 발생: " + t.getMessage());
            }
        };

        // WebSocket 연결
        client.newWebSocket(request, listener);
    }

    private static void loadMarkets() throws IOException {
        File file = Paths.get(DATA_FILE).toFile();
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
                new SubscriptionType("candle.1s", markets)
        );
        String jsonMessage = objectMapper.writeValueAsString(subscribeMsg);
        System.out.println("📡 전송할 구독 메시지: " + jsonMessage);

        // WebSocket을 통해 구독 메시지 전송
        webSocket.send(jsonMessage);
    }

    // 구독 메시지에 필요한 클래스들
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