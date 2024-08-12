package org.elece.sql.tcp.proto;

import org.elece.tcp.proto.Proto;
import org.elece.tcp.proto.ProtoException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

class ProtoTest {
    @Test
    public void test_deserializeStatement() throws ProtoException {
        byte[] header = new byte[] {0x14, 0x0, 0x0, 0x0};
        String query = "SELECT * FROM table;";
        byte[] queryBytes = query.getBytes(StandardCharsets.UTF_8);

        byte[] combined = new byte[header.length + queryBytes.length];

        System.arraycopy(header, 0, combined, 0, header.length);
        System.arraycopy(queryBytes, 0, combined, header.length, queryBytes.length);

        InputStream inputStream = new ByteArrayInputStream(combined);

        String queryResult = Proto.deserialize(inputStream);
        Assertions.assertEquals(query, queryResult);
    }
}