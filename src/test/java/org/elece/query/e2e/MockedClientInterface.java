package org.elece.query.e2e;

import org.elece.thread.ClientInterface;
import org.elece.utils.BinaryUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MockedClientInterface implements ClientInterface {
    private final List<Response> responses;
    private SelectResponse selectResponse;

    public MockedClientInterface() {
        responses = new ArrayList<>();
    }

    @Override
    public void send(byte[] response) {
        String stringResponse = BinaryUtils.bytesToString(response, 0);
        for (ResponseType responseType : ResponseType.values()) {
            if (stringResponse.contains(responseType.getResponseType())) {
                responses.add(new Response(responseType, stringResponse));
                return;
            }
        }

        if (stringResponse.contains("SelectInitialResult")) {
            selectResponse = new SelectResponse(stringResponse);
        } else if (stringResponse.contains("SelectEndResult")) {
            selectResponse.setEndHeader(stringResponse);
        } else if (Objects.nonNull(selectResponse)) {
            selectResponse.addRow(stringResponse);
        }
    }

    public SelectResponse getSelectResponse() {
        return selectResponse;
    }

    public List<Response> getResponses() {
        return responses;
    }

    public static final class SelectResponse {
        private final String initialHeader;
        private String endHeader;
        private final List<List<String>> rows;

        public SelectResponse(String initialHeader) {
            this.initialHeader = initialHeader;
            this.rows = new ArrayList<>();
        }

        public void addRow(String row) {
            String trimmedRow = row.substring(1, row.length() - 1);
            String[] splitValues = trimmedRow.split(", ");
            rows.add(List.of(splitValues));
        }

        public void setEndHeader(String endHeader) {
            this.endHeader = endHeader;
        }

        public String getInitialHeader() {
            return initialHeader;
        }

        public String getEndHeader() {
            return endHeader;
        }

        public List<List<String>> getRows() {
            return rows;
        }
    }

    public record Response(ResponseType responseType, String response) {
    }

    public enum ResponseType {
        CREATE_DB("CreateDbResult"),
        CREATE_INDEX("CreateIndexResult"),
        CREATE_TABLE("CreateTableResult"),
        DROP_DB("DropDbResult"),
        DROP_TABLE("DropTableResult"),
        DELETE("DeleteResult"),
        INSERT("InsertResult"),
        UPDATE("UpdateResult"),
        ERROR("ErrorResult");

        private final String responseType;

        ResponseType(String responseType) {
            this.responseType = responseType;
        }

        public String getResponseType() {
            return responseType;
        }
    }
}
