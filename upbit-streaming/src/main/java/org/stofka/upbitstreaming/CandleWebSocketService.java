package org.stofka.upbitstreaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.*;
import okio.ByteString;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
public class CandleWebSocketService {

    private static final String WEBSOCKET_URL = "wss://api.upbit.com/websocket/v1";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static List<String> markets = new ArrayList<>();

    private OkHttpClient client;
    private final KafkaSender kafkaSender;
    @Value("${app.data-file}")
    private String dataFilePath;

    public CandleWebSocketService(KafkaSender kafkaSender) {
        this.kafkaSender = kafkaSender;
    }

    @PostConstruct
    public void init() {
        client = new OkHttpClient();
        Request request = new Request.Builder().url(WEBSOCKET_URL).build();
        WebSocketListener listener = new WebSocketListener() {
            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                System.out.println("✅ Candle WebSocket 연결 성공!");

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

                    // JSON 파싱
                    JsonNode jsonNode = objectMapper.readTree(text);

                    // 스키마 기반 JSON 생성
                    ObjectNode schemaNode = objectMapper.createObjectNode();
                    schemaNode.put("type", "struct");

                    ArrayNode fieldsNode = objectMapper.createArrayNode();
                    fieldsNode.add(createFieldNode("type", "string"));
                    fieldsNode.add(createFieldNode("code", "string"));
                    fieldsNode.add(createFieldNode("opening_price", "double"));
                    fieldsNode.add(createFieldNode("high_price", "double"));
                    fieldsNode.add(createFieldNode("low_price", "double"));
                    fieldsNode.add(createFieldNode("trade_price", "double"));
                    fieldsNode.add(createFieldNode("candle_acc_trade_volume", "double"));
                    fieldsNode.add(createFieldNode("candle_acc_trade_price", "double"));
                    fieldsNode.add(createFieldNode("timestamp", "int64"));
                    fieldsNode.add(createFieldNode("stream_type", "string"));

                    schemaNode.set("fields", fieldsNode);
                    schemaNode.put("optional", false);
                    schemaNode.put("name", "market_candle");


                    // payload 생성
                    ObjectNode payloadNode = objectMapper.createObjectNode();
                    payloadNode.put("type", jsonNode.get("type").asText());
                    payloadNode.put("code", jsonNode.get("code").asText());
                    payloadNode.put("opening_price", jsonNode.get("opening_price").asDouble());
                    payloadNode.put("high_price", jsonNode.get("high_price").asDouble());
                    payloadNode.put("low_price", jsonNode.get("low_price").asDouble());
                    payloadNode.put("trade_price", jsonNode.get("trade_price").asDouble());
                    payloadNode.put("candle_acc_trade_volume", jsonNode.get("candle_acc_trade_volume").asDouble());
                    payloadNode.put("candle_acc_trade_price", jsonNode.get("candle_acc_trade_price").asDouble());
                    payloadNode.put("timestamp", jsonNode.get("timestamp").asLong());
                    payloadNode.put("stream_type", jsonNode.get("stream_type").asText());

                    // 최종 JSON 생성
                    ObjectNode finalJson = objectMapper.createObjectNode();
                    finalJson.set("schema", schemaNode);
                    finalJson.set("payload", payloadNode);

                    // 메시지 형식 지정
                    String logMessage = objectMapper.writeValueAsString(finalJson);
                    System.out.println(logMessage);

                    kafkaSender.send("candle", logMessage);  // "candle" 데이터 유형으로 전송

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

    private void loadMarkets() throws IOException {
        String classpathLocation = dataFilePath.replace("classpath:", "");
        ClassPathResource resource = new ClassPathResource(classpathLocation);

        try (InputStream is = resource.getInputStream()) {
            JsonNode jsonNode = objectMapper.readTree(is);

            for (JsonNode node : jsonNode) {
                String market = node.get("market").asText();
                markets.add(market);
            }
            System.out.println("📌 KRW 종목 로드 완료: " + markets.size() + "개");
        }
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

    // 필드 노드 생성 메서드
    private ObjectNode createFieldNode(String fieldName, String fieldType) {
        ObjectNode fieldNode = objectMapper.createObjectNode();
        fieldNode.put("field", fieldName);
        fieldNode.put("type", fieldType);
        return fieldNode;
    }
}