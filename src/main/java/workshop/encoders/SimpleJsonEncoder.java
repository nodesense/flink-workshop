package workshop.encoders;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.Encoder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

@PublicEvolving
public class SimpleJsonEncoder<IN> implements Encoder<IN> {
    private static final long serialVersionUID = -6865107843734614452L;

    ObjectMapper mapper = new ObjectMapper();
    public SimpleJsonEncoder() {

    }

    public void encode(IN element, OutputStream stream) throws IOException {
        String jsonStr = mapper.writeValueAsString(element);

        stream.write(jsonStr.getBytes());
        stream.write(10); // guessing new line \n
    }
}

