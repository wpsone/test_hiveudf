package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Description(name = "udfMD5",
    extended = "Example:\n > SELECT MD5('hashme') FROM potato;\n")
public class UDFMD5 extends UDF {
    public String evaluate(String message){
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(message.getBytes());
            byte[] message_digest = md.digest();
            BigInteger msg_int = new BigInteger(1, message_digest);
            String hash_string = msg_int.toString(16);
            while (hash_string.length()<32){
                hash_string = "0" + hash_string;
            }
            return hash_string;
        } catch (NoSuchAlgorithmException e) {
            return null;
        } catch (NullPointerException e){
            return null;
        }
    }
}
